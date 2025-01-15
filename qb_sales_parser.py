#!/usr/bin/env python3
import csv
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import re
from base_db import BaseDBHandler, logger
from rich.progress import Progress

@dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "US"

@dataclass
class QBSalesRecord:
    type: str
    date: str
    order_number: str
    source_name: str
    address_raw: str
    contact_name: str
    phone: str
    email: str
    memo: str
    name: str
    item: str
    item_description: str
    quantity: int
    price: float
    amount: float
    
    @property
    def channel(self) -> str:
        """Determine sales channel from order number format."""
        if re.match(r'\d{3}-\d{7}', self.order_number):
            return 'amazon'
        elif re.match(r'3D-\d{4}', self.order_number):
            return 'online_store'
        elif re.match(r'A\d{4}', self.order_number):
            return 'invoice'
        else:
            raise ValueError(f"Invalid order number format: {self.order_number}")
    
    @property
    def sku(self) -> str:
        """Extract SKU from item field."""
        match = re.match(r'([\w\-\.]+)\s*\(', self.item)
        if not match:
            raise ValueError(f"Unable to extract SKU from item: {self.item}")
        return match.group(1)

class AddressParser:
    """Parser for breaking down address strings into components."""
    
    @staticmethod
    def parse(address_str: str) -> Address:
        """Parse address string into components.
        
        Example: "1 Anystreet, Ri 02816-7613 US"
        """
        try:
            # Remove any leading/trailing commas and whitespace
            address_str = address_str.strip(' ,')
            
            # Match ZIP+4 or standard ZIP
            zip_match = re.search(r'(\d{5})(?:-\d{4})?', address_str)
            if not zip_match:
                raise ValueError(f"No ZIP code found in address: {address_str}")
            zip_code = zip_match.group(1)
            
            # Split remaining parts
            parts = [p.strip() for p in re.split(r',\s*', address_str)]
            
            if len(parts) < 2:
                raise ValueError(f"Invalid address format: {address_str}")
            
            # Last part should contain state and ZIP
            state_zip = parts[-1].split()
            if len(state_zip) < 2:
                raise ValueError(f"Invalid state/ZIP format: {parts[-1]}")
            
            state = state_zip[0]
            street = parts[0]
            city = parts[1] if len(parts) > 2 else ""
            
            return Address(
                street=street,
                city=city,
                state=state,
                zip_code=zip_code
            )
        except Exception as e:
            raise ValueError(f"Failed to parse address '{address_str}': {str(e)}")

class QBSalesParser(BaseDBHandler):
    """Parser for QuickBooks sales data CSV files."""
    
    def __init__(self, csv_path: str):
        super().__init__()
        self.csv_path = csv_path
        self.address_parser = AddressParser()
        self.errors: List[Dict[str, Any]] = []
    
    def parse_row(self, row: Dict[str, str], row_num: int) -> Optional[QBSalesRecord]:
        """Parse a single CSV row into a QBSalesRecord."""
        try:
            # Convert numeric fields
            quantity = int(float(row['Qty']))
            price = float(row['Sales Price'])
            amount = float(row['Amount'])
            
            return QBSalesRecord(
                type=row['Type'],
                date=row['Date'],
                order_number=row['Num'],
                source_name=row['Source Name'],
                address_raw=row['Name Address'],
                contact_name=row['Name Contact'],
                phone=row['Name Phone #'],
                email=row['Name E-Mail'],
                memo=row['Memo'],
                name=row['Name'],
                item=row['Item'],
                item_description=row['Item Description'],
                quantity=quantity,
                price=price,
                amount=amount
            )
        except (ValueError, KeyError) as e:
            self.errors.append({
                'row': row_num,
                'error': str(e),
                'data': row
            })
            return None
    
    def validate_record(self, record: QBSalesRecord, row_num: int) -> bool:
        """Validate a parsed record."""
        try:
            # Required validations
            if not record.email and record.source_name != 'Amazon FBA':
                raise ValueError("Missing email for non-Amazon FBA order")
            
            # These will raise ValueError if invalid
            _ = record.channel
            _ = record.sku
            
            # Parse address (will raise ValueError if invalid)
            _ = self.address_parser.parse(record.address_raw)
            
            return True
            
        except ValueError as e:
            self.errors.append({
                'row': row_num,
                'error': str(e),
                'record': record
            })
            return False
    
    def run(self):
        """Process the QuickBooks sales CSV file."""
        valid_records: List[QBSalesRecord] = []
        
        with self.progress:
            task_id = self.progress.add_task("Processing CSV file...", total=None)
            
            try:
                with open(self.csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row_num, row in enumerate(reader, start=1):
                        record = self.parse_row(row, row_num)
                        if record and self.validate_record(record, row_num):
                            valid_records.append(record)
                            
                        self.progress.update(task_id, advance=1)
                        
            except Exception as e:
                logger.error(f"Failed to process CSV file: {str(e)}")
                raise
            
            finally:
                # Update statistics
                self.stats.update({
                    'total_rows': row_num if 'row_num' in locals() else 0,
                    'valid_records': len(valid_records),
                    'errors': len(self.errors)
                })
                
                # Display results
                self.display_stats()
                
                if self.errors:
                    logger.warning(f"Found {len(self.errors)} errors during processing")
                    for error in self.errors:
                        logger.error(f"Row {error['row']}: {error['error']}")
                
        return valid_records

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python qb_sales_parser.py <csv_file>")
        sys.exit(1)
        
    parser = QBSalesParser(sys.argv[1])
    parser.run()
