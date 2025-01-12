#!/usr/bin/env python3
import os
import requests
from datetime import datetime
import time
import json
from decimal import Decimal
from base_db import BaseDBHandler, console
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.status import Status
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from typing import Optional, Dict, Any

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

class CompanyEnricher(BaseDBHandler):
    def __init__(self):
        super().__init__()
        self.api_token = os.getenv('COMPANIES_API_TOKEN')
        if not self.api_token:
            raise ValueError("COMPANIES_API_TOKEN environment variable is required")
        
        self.api_base_url = "https://api.thecompaniesapi.com/v2"
        self.excluded_domains = {'msn.com','hotmail.com', 'gmail.com', 'comcast.net', 'yahoo.com'}
        self.stats = {'processed': 0, 'success': 0, 'failed': 0}

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
                        (c.enrichment_data IS NULL OR c.enriched_date < NOW() - INTERVAL '1 week')
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
                company = {
                    'id': result[0],
                    'name': result[1],
                    'domain': result[2],
                    'total_sales': float(result[3]) if result[3] else 0.0
                }
                return company
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
                
                # Handle 404s specially
                if response.status_code == 404:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE companies
                            SET 
                                enrichment_data = %(enrichment_data)s::jsonb,
                                enrichment_source = 'thecompaniesapi.com',
                                enriched_date = NOW(),
                                updated_at = NOW()
                            WHERE id = %(company_id)s
                        """, {
                            'enrichment_data': json.dumps({
                                'error': 'not_found',
                                'status_code': 404,
                                'message': response.text
                            }),
                            'company_id': company['id']
                        })
                        self.conn.commit()
                    
                    console.print(f"[yellow]⚠[/yellow] Not found: {company['domain']}")
                    self.stats['failed'] += 1
                    return True  # Return True because we handled it properly
                
                # For other non-200 status codes, raise the error
                if response.status_code != 200:
                    console.print(f"[red]API Error ({response.status_code}):[/red] {response.text}")
                    response.raise_for_status()
                
                data = response.json()
                
                # Update the company record
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE companies
                        SET 
                            name = COALESCE(%(name)s, name),
                            enrichment_data = %(enrichment_data)s::jsonb,
                            enrichment_source = 'thecompaniesapi.com',
                            enriched_date = NOW(),
                            updated_at = NOW()
                        WHERE id = %(company_id)s
                    """, {
                        'name': data.get('name'),
                        'enrichment_data': json.dumps(data),
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

if __name__ == "__main__":
    try:
        with CompanyEnricher() as enricher:
            enricher.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Process interrupted by user[/yellow]")
        enricher.display_stats()
    except Exception as e:
        console.print("[bold red]Unexpected error occurred:[/bold red]")
        console.print_exception() 