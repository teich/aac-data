# Product Context

## Purpose
This project exists to import QuickBooks sales data into a structured database system. It processes CSV exports from QuickBooks containing sales records and creates normalized database records for people, companies, orders, and products.

## Problems Solved
1. Data Migration: Converts flat CSV data into normalized database records
2. Data Enrichment: Enhances raw data with additional structured information
3. Error Handling: Provides robust error detection and reporting for data quality
4. Contact Management: Sophisticated person/company matching and creation logic

## Expected Functionality
1. Parse QuickBooks CSV exports with specific format
2. Match or create person records using email/phone/name
3. Parse and store structured address information
4. Identify sales channels from order numbers
5. Match or create product records
6. Handle special cases like Amazon FBA orders and multi-email contacts
7. Provide detailed error reporting
