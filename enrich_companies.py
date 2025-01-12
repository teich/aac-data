#!/usr/bin/env python3
import os
import psycopg2
import requests
from datetime import datetime
import time
from typing import Optional, Dict, Any
import logging
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.status import Status
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up rich console and logging
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)

class CompanyEnricher:
    def __init__(self):
        self.conn = self.get_db_connection()
        self.api_token = os.getenv('COMPANIES_API_TOKEN')
        if not self.api_token:
            raise ValueError("COMPANIES_API_TOKEN environment variable is required")
        
        self.api_base_url = "https://api.thecompaniesapi.com/v2"
        self.excluded_domains = {'hotmail.com', 'gmail.com', 'comcast.net'}
        self.stats = {'processed': 0, 'success': 0, 'failed': 0}
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        )

    @staticmethod
    def get_db_connection():
        """Get database connection using environment variables."""
        return psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )

    def get_next_company(self) -> Optional[Dict[str, Any]]:
        """Get the next company to enrich, ordered by total sales."""
        with self.conn.cursor() as cur:
            cur.execute("""
                WITH company_sales AS (
                    SELECT 
                        c.id,
                        c.name,
                        c.domain,
                        COALESCE(SUM(o.amount), 0) as total_sales
                    FROM companies c
                    LEFT JOIN people p ON p.company_id = c.id
                    LEFT JOIN orders o ON o.person_id = p.id
                    WHERE 
                        c.enrichment_data IS NULL 
                        AND c.domain IS NOT NULL 
                        AND c.domain != ''
                        AND c.domain NOT IN %(excluded_domains)s
                        AND c.domain NOT LIKE '%%hotmail.%%'
                        AND c.domain NOT LIKE '%%gmail.%%'
                        AND c.domain NOT LIKE '%%comcast.%%'
                    GROUP BY c.id, c.name, c.domain
                )
                SELECT id, name, domain, total_sales
                FROM company_sales
                ORDER BY total_sales DESC
                LIMIT 1
            """, {'excluded_domains': tuple(self.excluded_domains)})
            
            result = cur.fetchone()
            if result:
                return {
                    'id': result[0],
                    'name': result[1],
                    'domain': result[2],
                    'total_sales': result[3]
                }
            return None

    def enrich_company(self, company: Dict[str, Any]) -> bool:
        """Enrich a single company using the API."""
        try:
            with Status(f"[bold blue]Enriching {company['domain']}...", console=console):
                headers = {
                    'Authorization': f'Basic {self.api_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.get(
                    f"{self.api_base_url}/companies/{company['domain']}",
                    headers=headers
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Update the company record
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE companies
                        SET 
                            name = COALESCE(%(name)s, name),
                            enrichment_data = %(enrichment_data)s,
                            enrichment_source = 'thecompaniesapi.com',
                            enriched_date = NOW(),
                            updated_at = NOW()
                        WHERE id = %(company_id)s
                    """, {
                        'name': data.get('name'),
                        'enrichment_data': response.json(),
                        'company_id': company['id']
                    })
                    self.conn.commit()
                
                self.stats['success'] += 1
                console.print(f"[green]✓[/green] Enriched: {company['domain']}")
                return True
                
        except Exception as e:
            self.stats['failed'] += 1
            console.print(f"[red]✗[/red] Failed: {company['domain']} - {str(e)}")
            self.conn.rollback()
            return False

    def display_company_info(self, company: Dict[str, Any]):
        """Display company information in a pretty table."""
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="yellow")
        
        table.add_row("Name", company['name'])
        table.add_row("Domain", company['domain'])
        table.add_row("Total Sales", f"${company['total_sales']:,.2f}")
        
        console.print(Panel(table, title="[bold]Company Details[/bold]", border_style="blue"))

    def display_stats(self):
        """Display enrichment statistics."""
        stats_table = Table(show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Count", style="yellow", justify="right")
        
        stats_table.add_row("Companies Processed", str(self.stats['processed']))
        stats_table.add_row("Successful Enrichments", str(self.stats['success']))
        stats_table.add_row("Failed Enrichments", str(self.stats['failed']))
        success_rate = (self.stats['success'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        stats_table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(Panel(stats_table, title="[bold]Enrichment Statistics[/bold]", border_style="green"))

    def run(self):
        """Main enrichment loop."""
        console.print(Panel.fit("[bold blue]Starting Company Enrichment Process[/bold blue]", border_style="blue"))
        
        try:
            while True:
                company = self.get_next_company()
                if not company:
                    console.print("[yellow]No more companies to enrich[/yellow]")
                    break
                
                self.stats['processed'] += 1
                self.display_company_info(company)
                success = self.enrich_company(company)
                
                # Add a small delay between requests to be nice to the API
                time.sleep(1)
            
        finally:
            self.display_stats()
            self.conn.close()

if __name__ == "__main__":
    try:
        enricher = CompanyEnricher()
        enricher.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Process interrupted by user[/yellow]")
        enricher.display_stats()
    except Exception as e:
        console.print("[bold red]Unexpected error occurred:[/bold red]")
        console.print_exception() 