#!/usr/bin/env python3
import os
import psycopg2
from typing import Optional, Dict, Any
import logging
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.status import Status
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.traceback import install
from dotenv import load_dotenv
from abc import ABC, abstractmethod

# Install rich traceback handler
install(show_locals=True)

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

class BaseDBHandler(ABC):
    """Base class for database operations with Rich console support."""
    
    def __init__(self):
        self.conn = self.get_db_connection()
        self.start_time = None
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        )
        self.stats: Dict[str, int] = {}

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

    def display_stats(self):
        """Display statistics in a Rich table."""
        if not self.stats:
            return
            
        stats_table = Table(show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Count", style="yellow", justify="right")
        
        for metric, count in self.stats.items():
            stats_table.add_row(metric.replace('_', ' ').title(), str(count))
            
        if 'processed' in self.stats and self.stats['processed'] > 0:
            success_rate = (self.stats.get('success', 0) / self.stats['processed'] * 100)
            stats_table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(Panel(stats_table, title="[bold]Statistics[/bold]", border_style="green"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        if exc_type:
            console.print("[bold red]Error occurred:[/bold red]")
            console.print_exception()
            return False
        return True

    @abstractmethod
    def run(self):
        """Main execution method to be implemented by subclasses."""
        pass 