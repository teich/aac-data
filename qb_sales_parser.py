#!/usr/bin/env python3
import csv
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import re
from base_db import BaseDBHandler, logger
from rich.progress import Progress
from datetime import datetime
import psycopg2
from typing import Optional, Tuple
from email.utils import parseaddr

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
        # Handle shipping as a special case
        if self.item.lower() == 'shipping':
            return 'shipping'
            
        # Extract SKU from parenthetical format
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
    
    def __init__(self, csv_path: str, dry_run: bool = False, line_limit: Optional[int] = None):
        super().__init__()
        self.csv_path = csv_path
        self.address_parser = AddressParser()
        self.errors: List[Dict[str, Any]] = []
        self.fba_user_counter = 0  # Counter for synthetic FBA users
        self.dry_run = dry_run
        self.line_limit = line_limit
        # By default disable progress bar and verbose logging
        # Only enable for dry runs
        self.progress.disable = not dry_run
        if not dry_run:
            logger.disabled = True
        self.simulated_ids = {
            'company': 1,
            'person': 1,
            'product': 1,
            'order': 1,
            'line_item': 1
        }
        self.operations_log: List[Dict[str, Any]] = []
    
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
    
    def get_domain_from_email(self, email: str) -> str:
        """Extract domain from email address."""
        _, email_part = parseaddr(email)
        return email_part.split('@')[1] if '@' in email_part else None

    def find_or_create_company(self, email: str) -> int:
        """Find or create company based on email domain."""
        if not email:
            return None
            
        domain = self.get_domain_from_email(email)
        if not domain:
            return None

        with self.conn.cursor() as cur:
            # Try to find existing company
            cur.execute(
                "SELECT id FROM companies WHERE domain = %s",
                (domain,)
            )
            result = cur.fetchone()
            
            if self.dry_run:
                self.operations_log.append({
                    'operation': 'company',
                    'domain': domain,
                    'status': 'found' if result else 'would_create',
                    'existing_id': result[0] if result else None,
                    'simulated_id': self.simulated_ids['company'] if not result else None
                })
                if result:
                    return result[0]
                company_id = self.simulated_ids['company']
                self.simulated_ids['company'] += 1
                return company_id
            
            # Non-dry run mode
            # Try to find existing company
            cur.execute(
                "SELECT id FROM companies WHERE domain = %s",
                (domain,)
            )
            result = cur.fetchone()
            
            if result:
                return result[0]
                
            # Create new company
            cur.execute(
                """
                INSERT INTO companies (name, domain)
                VALUES (%s, %s)
                RETURNING id
                """,
                (domain, domain)
            )
            return cur.fetchone()[0]

    def find_person(self, email: str, phone: str, name: str) -> Optional[Tuple[int, str]]:
        """Find person by email, phone, or exact name match. Returns tuple of (id, match_type)."""
        with self.conn.cursor() as cur:
            # Try email match first
            if email:
                cur.execute("SELECT id FROM people WHERE email = %s", (email,))
                result = cur.fetchone()
                if result:
                    return (result[0], 'email')
            
            # Try phone match
            if phone:
                cur.execute("SELECT id FROM people WHERE phone = %s", (phone,))
                result = cur.fetchone()
                if result:
                    return (result[0], 'phone')
            
            # Try exact name match
            if name:
                cur.execute("SELECT id FROM people WHERE name = %s", (name,))
                result = cur.fetchone()
                if result:
                    return (result[0], 'name')
            
            return None

    def create_person(self, record: QBSalesRecord, email: str, company_id: int) -> int:
        """Create a new person record."""
        address = self.address_parser.parse(record.address_raw)
        
        # First try to find existing person
        found = self.find_person(email, record.phone, record.name)
        
        if self.dry_run:
            self.operations_log.append({
                'operation': 'person',
                'name': record.name,
                'email': email,
                'company_id': company_id,
                'status': f'found_by_{found[1]}' if found else 'would_create',
                'existing_id': found[0] if found else None,
                'simulated_id': self.simulated_ids['person'] if not found else None
            })
            if found:
                return found[0]
            person_id = self.simulated_ids['person']
            self.simulated_ids['person'] += 1
            return person_id

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO people (
                    name, email, phone, address, city, state, zip, country,
                    company_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    record.name,
                    email,
                    record.phone,
                    address.street,
                    address.city,
                    address.state,
                    address.zip_code,
                    address.country,
                    company_id
                )
            )
            return cur.fetchone()[0]

    def handle_person(self, record: QBSalesRecord) -> int:
        """Handle person matching/creation logic."""
        # Handle Amazon FBA case
        if record.source_name == 'Amazon FBA' and not record.email:
            self.fba_user_counter += 1
            synthetic_email = f'FBA-user{self.fba_user_counter}@FBA-amazon.com'
            company_id = self.find_or_create_company(synthetic_email)
            return self.create_person(record, synthetic_email, company_id)
        
        # Handle multiple email addresses
        if ';' in record.email:
            emails = [e.strip() for e in record.email.split(';')]
            # Create person for first email
            company_id = self.find_or_create_company(emails[0])
            return self.create_person(record, emails[0], company_id)
        
        # Normal case
        person_id = self.find_person(record.email, record.phone, record.name)
        if person_id:
            return person_id
            
        company_id = self.find_or_create_company(record.email)
        return self.create_person(record, record.email, company_id)

    def find_or_create_product(self, record: QBSalesRecord) -> int:
        """Find or create product based on SKU."""
        with self.conn.cursor() as cur:
            # Try to find existing product
            cur.execute("SELECT id FROM products WHERE sku = %s", (record.sku,))
            result = cur.fetchone()
            
            if self.dry_run:
                self.operations_log.append({
                    'operation': 'product',
                    'sku': record.sku,
                    'name': record.item,
                    'status': 'found' if result else 'would_create',
                    'existing_id': result[0] if result else None,
                    'simulated_id': self.simulated_ids['product'] if not result else None
                })
                if result:
                    return result[0]
                product_id = self.simulated_ids['product']
                self.simulated_ids['product'] += 1
                return product_id

            # Try to find existing product
            cur.execute("SELECT id FROM products WHERE sku = %s", (record.sku,))
            result = cur.fetchone()
            
            if result:
                return result[0]
            
            # Create new product
            cur.execute(
                """
                INSERT INTO products (name, description, sku)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (record.item, record.item_description, record.sku)
            )
            return cur.fetchone()[0]

    def create_order(self, record: QBSalesRecord, person_id: int, product_id: int) -> Tuple[int, int]:
        """Create order record and line item."""
        if self.dry_run:
            order_id = self.simulated_ids['order']
            line_item_id = self.simulated_ids['line_item']
            self.operations_log.append({
                'operation': 'create_order',
                'order_number': record.order_number,
                'person_id': person_id,
                'product_id': product_id,
                'amount': record.amount,
                'simulated_order_id': order_id,
                'simulated_line_item_id': line_item_id
            })
            self.simulated_ids['order'] += 1
            self.simulated_ids['line_item'] += 1
            return order_id, line_item_id

        with self.conn.cursor() as cur:
            # Create order
            cur.execute(
                """
                INSERT INTO orders (
                    person_id, date, amount, order_number, channel, source
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    person_id,
                    datetime.strptime(record.date, '%m/%d/%Y').date(),
                    record.amount,
                    record.order_number,
                    record.channel,
                    record.source_name
                )
            )
            order_id = cur.fetchone()[0]
            
            # Create line item
            cur.execute(
                """
                INSERT INTO line_items (
                    order_id, product_id, unit_price, quantity, amount
                )
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    order_id,
                    product_id,
                    record.price,
                    record.quantity,
                    record.amount
                )
            )
            line_item_id = cur.fetchone()[0]
            
            return order_id, line_item_id

    def process_record(self, record: QBSalesRecord) -> bool:
        """Process a single record, creating all necessary database records."""
        try:
            with self.conn:
                # Handle person and company
                person_id = self.handle_person(record)
                
                # Handle product
                product_id = self.find_or_create_product(record)
                
                # Create order and line item
                order_id, line_item_id = self.create_order(record, person_id, product_id)
                
                # Remove per-order logging in non-dry-run mode
                if self.dry_run:
                    logger.info(
                        f"Created order {order_id} with line item {line_item_id} "
                        f"for person {person_id} and product {product_id}"
                    )
                
                return True
                
        except Exception as e:
            self.errors.append({
                'error': f"Failed to process record: {str(e)}",
                'record': record
            })
            return False

    def run(self):
        """Process the QuickBooks sales CSV file."""
        valid_records: List[QBSalesRecord] = []
        processed_count = 0
        
        with self.progress:
            task_id = self.progress.add_task("Processing CSV file...", total=None)
            
            try:
                # Try different encodings since QuickBooks files may not be UTF-8
                encodings = ['utf-8', 'cp1252', 'latin1', 'iso-8859-1']
                file_content = None
                
                for encoding in encodings:
                    try:
                        with open(self.csv_path, 'r', encoding=encoding) as f:
                            file_content = f.read()
                            break
                    except UnicodeDecodeError:
                        continue
                
                if file_content is None:
                    raise ValueError(f"Could not decode file {self.csv_path} with any of the attempted encodings")
                
                reader = csv.DictReader(file_content.splitlines())
                for row_num, row in enumerate(reader, start=1):
                    record = self.parse_row(row, row_num)
                    if record and self.validate_record(record, row_num):
                        valid_records.append(record)
                        processed_count += 1
                        
                        if self.line_limit and processed_count >= self.line_limit:
                            logger.info(f"Reached line limit of {self.line_limit}")
                            break
                        
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
                
                # Only show stats in dry run mode or if there are errors
                if self.dry_run or self.errors:
                    self.display_stats()
                    
                    if self.errors:
                        logger.warning(f"Found {len(self.errors)} errors during processing")
                        for error in self.errors:
                            logger.error(f"Row {error['row']}: {error['error']}")
                
            # Process valid records
            for record in valid_records:
                if not self.process_record(record):
                    logger.error(f"Failed to process record: {record}")

        return valid_records

if __name__ == '__main__':
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse QuickBooks sales data')
    parser.add_argument('csv_file', help='Path to the CSV file to process')
    parser.add_argument('--dry-run', action='store_true', help='Simulate processing without writing to database')
    parser.add_argument('--limit', type=int, help='Limit number of lines to process')
    
    args = parser.parse_args()
    
    qb_parser = QBSalesParser(args.csv_file, dry_run=args.dry_run, line_limit=args.limit)
    valid_records = qb_parser.run()
    
    if args.dry_run:
        # Group operations by order number for cleaner output
        order_groups = {}
        
        for op in qb_parser.operations_log:
            if op['operation'] == 'create_order':
                order_num = op['order_number']
                if order_num not in order_groups:
                    order_groups[order_num] = {'order': op, 'operations': []}
            else:
                # Add non-order operations to the most recent order group
                if order_groups:
                    latest_order = list(order_groups.values())[-1]
                    latest_order['operations'].append(op)
        
        print("\n=== QuickBooks Sales Parser Dry Run Summary ===\n")
        
        # Print unique products first
        products = {}
        for op in qb_parser.operations_log:
            if op['operation'] == 'product':
                products[op['sku']] = op
        
        if products:
            print("Products:")
            print("-" * 50)
            for sku, op in sorted(products.items()):
                if op['status'] == 'found':
                    print(f"✓ Found product: {op['name']}")
                    print(f"  SKU: {op['sku']}")
                    print(f"  ID: {op['existing_id']}")
                else:
                    print(f"+ Would create product: {op['name']}")
                    print(f"  SKU: {op['sku']}")
                    print(f"  ID: {op['simulated_id']}")
                print()
        
        # Print orders and their related operations
        print("Orders:")
        print("-" * 50)
        for order_num, group in order_groups.items():
            order = group['order']
            print(f"\nOrder {order_num}:")
            print(f"  Amount: ${order['amount']:.2f}")
            print(f"  Order ID: {order['simulated_order_id']}")
            print(f"  Line Item ID: {order['simulated_line_item_id']}")
            
            # Print related operations
            for op in group['operations']:
                if op['operation'] == 'company':
                    if op['status'] == 'found':
                        print(f"  ✓ Found company: {op['domain']}")
                        print(f"    ID: {op['existing_id']}")
                    else:
                        print(f"  + Would create company: {op['domain']}")
                        print(f"    ID: {op['simulated_id']}")
                elif op['operation'] == 'person':
                    if 'found' in op['status']:
                        match_type = op['status'].split('_')[2]
                        print(f"  ✓ Found person by {match_type}: {op['name']}")
                        print(f"    Email: {op['email']}")
                        print(f"    ID: {op['existing_id']}")
                    else:
                        print(f"  + Would create person: {op['name']}")
                        print(f"    Email: {op['email']}")
                        print(f"    ID: {op['simulated_id']}")
                elif op['operation'] == 'product':
                    # Skip products as they're shown in the summary above
                    pass
            print("  " + "-" * 48)
