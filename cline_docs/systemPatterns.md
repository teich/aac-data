# System Patterns

## Architecture
- Python-based data processing system
- PostgreSQL database
- Sqitch for database migrations

## Key Technical Decisions
1. Database-First Approach
   - Structured schema design
   - Strong data validation
   - Efficient indexing

2. Data Processing Pipeline
   - CSV parsing
   - Data validation
   - Record matching/creation
   - Error handling/reporting

3. Modular Design
   - Separate SQL migrations
   - Distinct processing phases
   - Clear error handling strategy

## Implementation Patterns
1. Database Schema
   - Normalized tables (people, companies, orders, products)
   - Explicit constraints and relationships
   - Migration-based schema evolution

2. Data Processing
   - Validation before processing
   - Hierarchical record creation (people -> companies -> orders -> line items)
   - Comprehensive error logging

3. Error Handling
   - Detailed error reporting
   - Critical error stops
   - Error categorization
   - Row-level error tracking
