#!/usr/bin/env python3
import os
import requests
from datetime import datetime
import time
import json
import argparse
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

class PeopleEnricher(BaseDBHandler):
    def __init__(self):
        super().__init__()
        self.api_token = os.getenv('PEOPLEDATALAB_API_TOKEN')
        if not self.api_token:
            raise ValueError("PEOPLEDATALAB_API_TOKEN environment variable is required")
        
        self.api_url = "https://api.peopledatalabs.com/v5/person/enrich"
        self.excluded_domains = {'msn.com', 'hotmail.com', 'gmail.com', 'comcast.net', 'yahoo.com'}
        self.stats = {'processed': 0, 'success': 0, 'failed': 0}

    def get_next_person(self) -> Optional[Dict[str, Any]]:
        """Get the next person to enrich, ordered by total orders."""
        with self.conn.cursor() as cur:
            cur.execute("""
                WITH person_orders AS (
                    SELECT 
                        p.id,
                        p.name,
                        p.email,
                        c.domain as company_domain,
                        COALESCE(SUM(o.amount), 0) as total_orders
                    FROM people p
                    JOIN companies c ON c.id = p.company_id
                    LEFT JOIN orders o ON o.person_id = p.id
                    WHERE 
                        (p.enrichment_data IS NULL OR p.enriched_date < NOW() - INTERVAL '1 week')
                        AND p.email IS NOT NULL 
                        AND p.email != ''
                        AND SPLIT_PART(p.email, '@', 2) NOT IN %(excluded_domains)s
                    GROUP BY p.id, p.name, p.email, c.domain
                )
                SELECT id, name, email, company_domain, total_orders
                FROM person_orders
                ORDER BY total_orders DESC
                LIMIT 1
            """, {'excluded_domains': tuple(self.excluded_domains)})
            
            result = cur.fetchone()
            if result:
                person = {
                    'id': result[0],
                    'name': result[1],
                    'email': result[2],
                    'company_domain': result[3],
                    'total_orders': float(result[4]) if result[4] else 0.0
                }
                return person
            return None

    def enrich_person(self, person: Dict[str, Any]) -> bool:
        """Enrich a single person using the PeopleDataLabs API."""
        try:
            with Status(f"[bold blue]Enriching {person['email']}...", console=console):
                headers = {
                    'X-Api-Key': self.api_token,
                    'Content-Type': 'application/json'
                }
                
                params = {
                    'email': person['email'],
                    'pretty': 'false',
                    'min_likelihood': '2',
                    'include_if_matched': 'false',
                    'titlecase': 'false'
                }
                
                response = requests.get(
                    self.api_url,
                    headers=headers,
                    params=params
                )
                
                # Handle 404s specially
                if response.status_code == 404:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE people
                            SET 
                                enrichment_data = %(enrichment_data)s::jsonb,
                                enrichment_source = 'peopledatalabs.com',
                                enriched_date = NOW(),
                                updated_at = NOW()
                            WHERE id = %(person_id)s
                        """, {
                            'enrichment_data': json.dumps({
                                'error': 'not_found',
                                'status_code': 404,
                                'message': response.text
                            }),
                            'person_id': person['id']
                        })
                        self.conn.commit()
                    
                    console.print(f"[yellow]⚠[/yellow] Not found: {person['email']}")
                    self.stats['failed'] += 1
                    return True  # Return True because we handled it properly
                
                # For other non-200 status codes, raise the error
                if response.status_code != 200:
                    console.print(f"[red]API Error ({response.status_code}):[/red] {response.text}")
                    response.raise_for_status()
                
                data = response.json()
                
                # Update the person record
                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE people
                        SET 
                            name = COALESCE(%(name)s, name),
                            enrichment_data = %(enrichment_data)s::jsonb,
                            enrichment_source = 'peopledatalabs.com',
                            enriched_date = NOW(),
                            updated_at = NOW()
                        WHERE id = %(person_id)s
                    """, {
                        'name': data.get('full_name'),
                        'enrichment_data': json.dumps(data),
                        'person_id': person['id']
                    })
                    self.conn.commit()
                
                self.stats['success'] += 1
                console.print(f"[green]✓[/green] Enriched: {person['email']}")
                return True
                
        except Exception as e:
            self.stats['failed'] += 1
            console.print(f"[red]✗[/red] Failed: {person['email']} - {str(e)}")
            self.conn.rollback()
            return False

    def display_person_info(self, person: Dict[str, Any]):
        """Display person information in a pretty table."""
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="yellow")
        
        table.add_row("Name", person['name'])
        table.add_row("Email", person['email'])
        table.add_row("Company Domain", person['company_domain'])
        table.add_row("Total Orders", f"${person['total_orders']:,.2f}")
        
        console.print(Panel(table, title="[bold]Person Details[/bold]", border_style="blue"))

    def display_stats(self):
        """Display enrichment statistics."""
        stats_table = Table(show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Count", style="yellow", justify="right")
        
        stats_table.add_row("People Processed", str(self.stats['processed']))
        stats_table.add_row("Successful Enrichments", str(self.stats['success']))
        stats_table.add_row("Failed Enrichments", str(self.stats['failed']))
        success_rate = (self.stats['success'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        stats_table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(Panel(stats_table, title="[bold]Enrichment Statistics[/bold]", border_style="green"))

    def run(self, num_records=None):
        """Main enrichment loop."""
        console.print(Panel.fit("[bold blue]Starting People Enrichment Process[/bold blue]", border_style="blue"))
        
        try:
            records_processed = 0
            while True:
                if num_records is not None and records_processed >= num_records:
                    console.print(f"[yellow]Processed requested number of records ({num_records})[/yellow]")
                    break

                person = self.get_next_person()
                if not person:
                    console.print("[yellow]No more people to enrich[/yellow]")
                    break
                
                self.stats['processed'] += 1
                records_processed += 1
                self.display_person_info(person)
                success = self.enrich_person(person)
                
                # Add a small delay between requests to be nice to the API
                time.sleep(0.1)
            
        finally:
            self.display_stats()

def parse_args():
    parser = argparse.ArgumentParser(description='Enrich people records using PeopleDataLabs API')
    parser.add_argument(
        '-n', '--num-records',
        type=int,
        default=None,
        help='Number of records to process (default: process all records)'
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        with PeopleEnricher() as enricher:
            enricher.run(args.num_records)
    except KeyboardInterrupt:
        console.print("\n[yellow]Process interrupted by user[/yellow]")
        enricher.display_stats()
    except Exception as e:
        console.print("[bold red]Unexpected error occurred:[/bold red]")
        console.print_exception()
