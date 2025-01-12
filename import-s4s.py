import pandas as pd
from psycopg2.extras import execute_values
import sys
from typing import Dict, Tuple, List
import os
from time import time
import argparse
from base_db import BaseDBHandler, console

class BaseImporter(BaseDBHandler):
    def read_csv(self, csv_path: str) -> pd.DataFrame:
        """Read and return the CSV file as a DataFrame."""
        with console.status("[bold green]Reading CSV file..."):
            try:
                # First try UTF-8
                df = pd.read_csv(csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    # Then try cp1252 (Windows default)
                    df = pd.read_csv(csv_path, encoding='cp1252')
                except UnicodeDecodeError:
                    # Finally try latin1 (should work for most cases)
                    df = pd.read_csv(csv_path, encoding='latin1')
            
            console.print(f"[green]✓[/green] Loaded {len(df):,} records")
            return df

class PeopleImporter(BaseImporter):
    def ensure_unknown_company(self) -> int:
        """Ensure the unknown company exists and return its ID."""
        with self.conn.cursor() as cur:
            # First try to find existing unknown company
            cur.execute("SELECT id FROM companies WHERE domain = 'unknown'")
            result = cur.fetchone()
            if result:
                return result[0]
                
            # Create if it doesn't exist
            cur.execute("""
                INSERT INTO companies (name, domain)
                VALUES ('Unknown Company', 'unknown')
                RETURNING id
            """)
            return cur.fetchone()[0]

    def ensure_companies(self, company_data: List[Tuple[str, str]]) -> Dict[str, int]:
        """Batch process companies, returning domain -> id mapping."""
        with self.conn.cursor() as cur:
            # Create temp table for companies
            cur.execute("""
                CREATE TEMP TABLE tmp_companies (
                    name TEXT,
                    domain TEXT PRIMARY KEY
                ) ON COMMIT DROP
            """)
            
            # Filter out None/nan domains and bulk insert into temp table
            valid_company_data = [(name, domain) for name, domain in company_data if pd.notna(domain)]
            if valid_company_data:
                execute_values(cur, """
                    INSERT INTO tmp_companies (name, domain)
                    VALUES %s
                    ON CONFLICT (domain) DO NOTHING
                """, valid_company_data)
            
            # Insert new companies and get all company IDs
            cur.execute("""
                WITH new_companies AS (
                    INSERT INTO companies (name, domain)
                    SELECT t.name, t.domain FROM tmp_companies t
                    LEFT JOIN companies c ON c.domain = t.domain
                    WHERE c.id IS NULL
                    RETURNING domain, id
                )
                SELECT c.domain, c.id FROM companies c
                WHERE c.domain IN (SELECT domain FROM tmp_companies)
                UNION ALL
                SELECT domain, id FROM new_companies
            """)
            
            return dict(cur.fetchall())

    def run(self, csv_path: str):
        """Process people import."""
        df = self.read_csv(csv_path)
        
        # Clean up whitespace and combine names
        df['first'] = df['first'].str.strip()
        df['last'] = df['last'].str.strip()
        df['name'] = df['first'] + ' ' + df['last']
        
        # Ensure unknown company exists
        unknown_company_id = self.ensure_unknown_company()
        
        # First ensure all companies exist
        print("Processing companies...", end='', flush=True)
        company_data = list(set(zip(df['company'], df['domain'])))
        domain_to_id = self.ensure_companies(company_data)
        print(f" done. {len(domain_to_id)} companies processed.")
        
        print("Importing people...", end='', flush=True)
        # Prepare people data, using unknown company for missing domains
        people_data = [
            (row['name'], row['email'], 
             domain_to_id.get(row['domain'], unknown_company_id) if pd.notna(row['domain']) else unknown_company_id)
            for _, row in df.iterrows()
        ]
        
        # Bulk insert people
        with self.conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO people (name, email, company_id)
                VALUES %s
                ON CONFLICT (email) DO NOTHING
            """, people_data)
            
        self.conn.commit()
        print(f" done. {len(people_data)} people processed.")

class OrdersImporter(BaseImporter):
    def get_person_ids(self, emails: List[str]) -> Dict[str, int]:
        """Get person IDs by email."""
        with self.conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(emails))
            cur.execute(f"""
                SELECT email, id FROM people 
                WHERE email IN ({placeholders})
            """, list(emails))
            return dict(cur.fetchall())

    def process(self, csv_path: str):
        """Process orders import."""
        df = self.read_csv(csv_path)
        
        # Validate required columns
        required_columns = ['email', 'date', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Clean up data
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Get person IDs for all emails
        print("Looking up person IDs...", end='', flush=True)
        person_ids = self.get_person_ids(df['email'].unique())
        print(f" done. Found {len(person_ids)} matching people.")
        
        # Prepare order data
        orders_data = []
        skipped = 0
        for _, row in df.iterrows():
            person_id = person_ids.get(row['email'])
            if not person_id:
                skipped += 1
                continue
                
            orders_data.append((
                person_id,
                row['date'],
                row['amount']
            ))
        
        if skipped:
            print(f"Warning: Skipped {skipped} orders due to missing person records.")
        
        print(f"Importing orders...", end='', flush=True)
        with self.conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO orders (person_id, date, amount)
                VALUES %s
                RETURNING id
            """, orders_data)
            
        self.conn.commit()
        print(f" done. {len(orders_data)} orders processed.")

class ProductsImporter(BaseImporter):
    def process(self, csv_path: str):
        """Process products import."""
        df = self.read_csv(csv_path)
        
        # Validate required columns
        required_columns = ['name', 'sku']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Clean up data
        df['name'] = df['name'].str.strip()
        df['sku'] = df['sku'].str.strip()
        df['description'] = df['description'].fillna('')
        
        # Prepare product data
        products_data = [
            (row['name'], row['description'], row['sku'])
            for _, row in df.iterrows()
        ]
        
        print("Importing products...", end='', flush=True)
        with self.conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO products (name, description, sku)
                VALUES %s
                ON CONFLICT (sku) DO UPDATE 
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, products_data)
            
        self.conn.commit()
        print(f" done. {len(products_data)} products processed.")

class LineItemsImporter(BaseImporter):
    def get_order_ids(self, order_refs: List[str]) -> Dict[str, int]:
        """Get order IDs by reference (could be ID or other identifier)."""
        with self.conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(order_refs))
            cur.execute(f"""
                SELECT id FROM orders 
                WHERE id::text IN ({placeholders})
            """, list(order_refs))
            return {str(id): id for (id,) in cur.fetchall()}
    
    def get_product_ids(self, skus: List[str]) -> Dict[str, int]:
        """Get product IDs by SKU."""
        with self.conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(skus))
            cur.execute(f"""
                SELECT sku, id FROM products 
                WHERE sku IN ({placeholders})
            """, list(skus))
            return dict(cur.fetchall())

    def process(self, csv_path: str):
        """Process line items import."""
        df = self.read_csv(csv_path)
        
        # Validate required columns
        required_columns = ['order_id', 'sku', 'quantity', 'unit_price']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Clean up data
        df['order_id'] = df['order_id'].astype(str)
        df['sku'] = df['sku'].str.strip()
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df['amount'] = df['quantity'] * df['unit_price']
        
        # Get order and product IDs
        print("Looking up order and product IDs...", end='', flush=True)
        order_ids = self.get_order_ids(df['order_id'].unique())
        product_ids = self.get_product_ids(df['sku'].unique())
        print(f" done. Found {len(order_ids)} orders and {len(product_ids)} products.")
        
        # Prepare line item data
        line_items_data = []
        skipped = 0
        for _, row in df.iterrows():
            order_id = order_ids.get(row['order_id'])
            product_id = product_ids.get(row['sku'])
            
            if not order_id or not product_id:
                skipped += 1
                continue
                
            line_items_data.append((
                order_id,
                product_id,
                row['unit_price'],
                row['quantity'],
                row['amount']
            ))
        
        if skipped:
            print(f"Warning: Skipped {skipped} line items due to missing order or product records.")
        
        print(f"Importing line items...", end='', flush=True)
        with self.conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO line_items (order_id, product_id, unit_price, quantity, amount)
                VALUES %s
            """, line_items_data)
            
        self.conn.commit()
        print(f" done. {len(line_items_data)} line items processed.")

class CombinedOrderImporter(BaseImporter):
    def ensure_unknown_company(self) -> int:
        """Ensure the unknown company exists and return its ID."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM companies WHERE domain = 'unknown'")
            result = cur.fetchone()
            if result:
                return result[0]
            
            cur.execute("""
                INSERT INTO companies (name, domain)
                VALUES ('Unknown Company', 'unknown')
                RETURNING id
            """)
            return cur.fetchone()[0]

    def ensure_companies(self, company_data: List[Tuple[str, str]]) -> Dict[str, int]:
        """Batch process companies, returning domain -> id mapping."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_companies (
                    name TEXT,
                    domain TEXT PRIMARY KEY
                ) ON COMMIT DROP
            """)
            
            valid_company_data = [(name, domain) for name, domain in company_data if pd.notna(domain)]
            if valid_company_data:
                execute_values(cur, """
                    INSERT INTO tmp_companies (name, domain)
                    VALUES %s
                    ON CONFLICT (domain) DO NOTHING
                """, valid_company_data)
            
            cur.execute("""
                WITH new_companies AS (
                    INSERT INTO companies (name, domain)
                    SELECT t.name, t.domain FROM tmp_companies t
                    LEFT JOIN companies c ON c.domain = t.domain
                    WHERE c.id IS NULL
                    RETURNING domain, id
                )
                SELECT c.domain, c.id FROM companies c
                WHERE c.domain IN (SELECT domain FROM tmp_companies)
                UNION ALL
                SELECT domain, id FROM new_companies
            """)
            
            return dict(cur.fetchall())

    def ensure_people(self, people_data: List[Tuple[str, str, str, str, str]]) -> Dict[str, int]:
        """Ensure people exist and return email -> id mapping."""
        with self.conn.cursor() as cur:
            # First ensure companies exist
            company_data = list(set((company, domain) for _, _, _, company, domain in people_data))
            domain_to_id = self.ensure_companies(company_data)
            unknown_company_id = self.ensure_unknown_company()
            
            # Create temp table for people
            cur.execute("""
                CREATE TEMP TABLE tmp_people (
                    name TEXT,
                    email TEXT PRIMARY KEY,
                    company_id INTEGER
                ) ON COMMIT DROP
            """)
            
            # Prepare people records
            people_records = [
                (
                    f"{first} {last}".strip(),
                    email,
                    domain_to_id.get(domain, unknown_company_id) if pd.notna(domain) else unknown_company_id
                )
                for first, last, email, _, domain in people_data
                if pd.notna(email)
            ]
            
            if people_records:
                execute_values(cur, """
                    INSERT INTO tmp_people (name, email, company_id)
                    VALUES %s
                    ON CONFLICT (email) DO NOTHING
                """, people_records)
            
            # Insert new people and get all IDs
            cur.execute("""
                WITH new_people AS (
                    INSERT INTO people (name, email, company_id)
                    SELECT t.name, t.email, t.company_id FROM tmp_people t
                    LEFT JOIN people p ON p.email = t.email
                    WHERE p.id IS NULL
                    RETURNING email, id
                )
                SELECT p.email, p.id FROM people p
                WHERE p.email IN (SELECT email FROM tmp_people)
                UNION ALL
                SELECT email, id FROM new_people
            """)
            
            return dict(cur.fetchall())

    def ensure_products(self, product_data: List[Tuple[str, str]]) -> Dict[str, int]:
        """Ensure products exist and return sku -> id mapping."""
        with self.conn.cursor() as cur:
            with console.status("[bold yellow]Processing products...") as status:
                cur.execute("""
                    CREATE TEMP TABLE tmp_products (
                        name TEXT,
                        sku TEXT PRIMARY KEY
                    ) ON COMMIT DROP
                """)
                
                if product_data:
                    execute_values(cur, """
                        INSERT INTO tmp_products (name, sku)
                        VALUES %s
                        ON CONFLICT (sku) DO NOTHING
                    """, product_data)
                    
                    cur.execute("""
                        WITH new_products AS (
                            INSERT INTO products (name, sku)
                            SELECT t.name, t.sku FROM tmp_products t
                            LEFT JOIN products p ON p.sku = t.sku
                            WHERE p.id IS NULL
                            RETURNING sku, id
                        )
                        SELECT p.sku, p.id FROM products p
                        WHERE p.sku IN (SELECT sku FROM tmp_products)
                        UNION ALL
                        SELECT sku, id FROM new_products
                    """)
                    
                    result = dict(cur.fetchall())
                    console.print(f"[green]✓[/green] Processed {len(result):,} products")
                    return result
                else:
                    console.print("[yellow]![/yellow] No product data provided")
                    return {}

    def process(self, csv_path: str):
        """Process the combined order import."""
        df = self.read_csv(csv_path)
        
        # Validate required columns
        required_columns = [
            'odate', 'orderamount', 'ofirstname', 'olastname', 'oemail',
            'Domain', 'ocompany', 'itemid', 'itemname', 'numitems', 'unitprice',
            'invoicenum'  # Add invoice number to required columns
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Clean up data
        with console.status("[bold yellow]Preprocessing data..."):
            df['odate'] = pd.to_datetime(df['odate'])
            df['orderamount'] = pd.to_numeric(df['orderamount'], errors='coerce')
            df['numitems'] = pd.to_numeric(df['numitems'], errors='coerce')
            df['unitprice'] = pd.to_numeric(df['unitprice'], errors='coerce')
            df['itemamount'] = df['numitems'] * df['unitprice']
            df['invoicenum'] = df['invoicenum'].astype(str)  # Ensure invoice numbers are strings
        
        # Process people and companies
        with console.status("[bold yellow]Processing people and companies...") as status:
            people_data = list(set(zip(
                df['ofirstname'], df['olastname'], df['oemail'],
                df['ocompany'], df['Domain']
            )))
            email_to_id = self.ensure_people(people_data)
            if not email_to_id:
                raise ValueError("No people were created or found")
            console.print(f"[green]✓[/green] Processed {len(email_to_id):,} people")
        
        # Process products
        product_data = list(set(zip(df['itemname'], df['itemid'])))
        sku_to_id = self.ensure_products(product_data)
        if not sku_to_id:
            raise ValueError("No products were created or found")
        
        # Process orders and line items
        orders_created = 0
        orders_skipped = 0
        line_items_created = 0
        
        # Get existing invoice numbers
        with self.conn.cursor() as cur:
            cur.execute("SELECT invoice_number FROM orders WHERE invoice_number IS NOT NULL")
            existing_invoices = {row[0] for row in cur.fetchall()}
        
        # Prepare batches
        order_batch = []
        line_items_batch = []
        total_orders = len(df.groupby(['odate', 'oemail', 'orderamount', 'invoicenum']))
        
        with self.progress:
            task = self.progress.add_task("[yellow]Processing orders...", total=total_orders)
            
            for i, ((date, email, amount, invoice_num), order_items) in enumerate(
                df.groupby(['odate', 'oemail', 'orderamount', 'invoicenum']), 1
            ):
                # Skip if invoice already exists
                if invoice_num in existing_invoices:
                    orders_skipped += 1
                    self.progress.update(task, advance=1)
                    continue
                
                person_id = email_to_id.get(email)
                if not person_id:
                    raise ValueError(f"Person not found for email: {email}")
                
                # Convert amount to Python float
                amount = float(amount) if pd.notna(amount) else 0.0
                
                # Add to order batch
                order_batch.append((person_id, date.strftime('%Y-%m-%d'), amount, invoice_num))
                
                # Prepare line items for this order
                for _, item in order_items.iterrows():
                    product_id = sku_to_id.get(item['itemid'])
                    if not product_id:
                        raise ValueError(f"Product not found for SKU: {item['itemid']}")
                    
                    unit_price = float(item['unitprice']) if pd.notna(item['unitprice']) else 0.0
                    quantity = int(item['numitems']) if pd.notna(item['numitems']) else 0
                    item_amount = float(item['itemamount']) if pd.notna(item['itemamount']) else 0.0
                    
                    line_items_batch.append({
                        'order_index': len(order_batch) - 1,
                        'product_id': product_id,
                        'unit_price': unit_price,
                        'quantity': quantity,
                        'amount': item_amount
                    })
                
                # Process in batches
                if len(order_batch) >= 50:
                    try:
                        self._process_batch(order_batch, line_items_batch)
                        orders_created += len(order_batch)
                        line_items_created += len(line_items_batch)
                        # Add processed invoices to existing set
                        existing_invoices.update(invoice for _, _, _, invoice in order_batch)
                        order_batch = []
                        line_items_batch = []
                    except Exception as e:
                        console.print(f"\n[bold red]Error processing batch:[/bold red] {str(e)}")
                        raise
                
                self.progress.update(task, advance=1)
        
        # Process remaining batch
        if order_batch:
            try:
                self._process_batch(order_batch, line_items_batch)
                orders_created += len(order_batch)
                line_items_created += len(line_items_batch)
            except Exception as e:
                console.print(f"\n[bold red]Error processing final batch:[/bold red] {str(e)}")
                raise
        
        console.print(Panel(f"[bold green]Import Summary[/bold green]\n"
                          f"Orders created: {orders_created:,}\n"
                          f"Orders skipped (already exist): {orders_skipped:,}\n"
                          f"Line items created: {line_items_created:,}"))
            
    def _process_batch(self, order_batch, line_items_batch):
        """Process a batch of orders and their line items."""
        with self.conn.cursor() as cur:
            try:
                # Insert orders with invoice numbers
                execute_values(cur, """
                    INSERT INTO orders (person_id, date, amount, invoice_number)
                    VALUES %s
                    ON CONFLICT (invoice_number) DO NOTHING
                    RETURNING id, invoice_number
                """, order_batch, page_size=50)
                
                # Get the order IDs that were just inserted
                new_orders = dict(cur.fetchall())
                
                # Get all order IDs (both new and existing) by invoice numbers
                invoice_numbers = [invoice for _, _, _, invoice in order_batch]
                placeholders = ','.join(['%s'] * len(invoice_numbers))
                cur.execute(f"""
                    SELECT id, invoice_number FROM orders 
                    WHERE invoice_number IN ({placeholders})
                """, invoice_numbers)
                # Create a reverse mapping from invoice_number to id
                all_orders = {invoice: id for id, invoice in cur.fetchall()}
                
                # Create a mapping of batch index to order ID
                order_id_map = {}
                for i, (_, _, _, invoice) in enumerate(order_batch):
                    if invoice in all_orders:
                        order_id_map[i] = all_orders[invoice]
                
                # Process line items for all orders (both new and existing)
                if line_items_batch:
                    line_items = []
                    for item in line_items_batch:
                        order_index = item['order_index']
                        order_id = order_id_map.get(order_index)
                        if not order_id:
                            continue
                            
                        line_items.append((
                            order_id,
                            item['product_id'],
                            item['unit_price'],
                            item['quantity'],
                            item['amount']
                        ))
                    
                    if line_items:
                        execute_values(cur, """
                            INSERT INTO line_items (order_id, product_id, unit_price, quantity, amount)
                            VALUES %s
                        """, line_items, page_size=100)
                
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                raise

def main():
    parser = argparse.ArgumentParser(description='Import data into the database')
    parser.add_argument('model', choices=['people', 'orders', 'products', 'line-items', 'combined-order'], 
                       help='The model type to import')
    parser.add_argument('csv_file', help='Path to the CSV file to import')
    
    args = parser.parse_args()
    
    console.print(Panel(f"[bold]Starting import of [cyan]{args.model}[/cyan] from [yellow]{args.csv_file}[/yellow][/bold]"))
    
    importers = {
        'people': PeopleImporter,
        'orders': OrdersImporter,
        'products': ProductsImporter,
        'line-items': LineItemsImporter,
        'combined-order': CombinedOrderImporter,
    }
    
    importer_class = importers.get(args.model)
    if not importer_class:
        console.print(f"[bold red]Error:[/bold red] Unknown model type: {args.model}")
        sys.exit(1)
    
    try:
        with importer_class() as importer:
            importer.run(args.csv_file)
    except FileNotFoundError:
        console.print(f"[bold red]Error:[/bold red] Could not find file: {args.csv_file}")
        sys.exit(1)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)
    except Exception as e:
        console.print("[bold red]Unexpected error occurred:[/bold red]")
        console.print_exception()
        sys.exit(1)

if __name__ == "__main__":
    main() 