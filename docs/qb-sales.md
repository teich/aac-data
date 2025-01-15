# Quickbook Sales Import Feature

Given a CSV file with the format:
```csv
,"Type","Date","Num","Source Name","Name Address","Name Contact","Name Phone #","Name E-Mail","Memo","Name","Item","Item Description","Qty","Sales Price","Amount"
,"Sales Receipt","11/25/2016","610-4148257",", Jody","1 Anystreet, Ri 02816-7613 US","Jody","4012615135","p23232232@marketplace.amazon.com","SP10-38 asphalt anchors, carton of 6 anchors",", Jody","01-6310.38K (SP10-38 asphalt anchors, carton of 6 anchors)","SP10-38 asphalt anchors, carton of 6 anchors",1,42.00,42.00
,"Sales Receipt","11/26/2016","912-3712214","Smith, Andrew","2 Anystreet, Ri 02917 US","Andrew Smith","1112223333","3323232@marketplace.amazon.com","SP10-38 asphalt anchors, carton of 6 anchors","Smith, Andrew","01-6310.38K (SP10-38 asphalt anchors, carton of 6 anchors)","SP10-38 asphalt anchors, carton of 6 anchors",1,42.00,42.00
```

We want to parse this and create the correct records in the database.

## Person

* Match the person. Not every field is available
  1. Match by email (exact match)
  2. Match by phone 
  3. Match by exact name including spelling
* If the person doesn't exist, create them
* For email addresses containing semicolons (e.g. "email1@example.com;email2@example.com"):
  * Create a separate person record for EACH email address
  * For these split-email cases:
    * Each person gets one email address
    * Leave name and phone blank
    * Copy the address details to each person
    * Each person gets their own company (based on email domain)
* For single-email cases:
  * Create one person with all available details (name, phone, email, address)
* Some orders, specifically from Source `Amazon FBA` won't have any contact info - create a sequenced synthetic user with a fake email address like 'FBA-user1@FBA-amazon.com. 
  * if you find a user without email who isn't Amazon FBA, error and stop processing.
* Users belong to companies. If you are creating a user, check if the company exists by matching on the domain name of their email address. If the company doesn't exist create it.

## Address Handling
* When creating a new person, parse the "Name Address" field into components:
  * Street address
  * City
  * State
  * ZIP
  * Country
* Store these components in the corresponding fields in the people table

## Orders
* Each order is specified by the 'Num' column. 
* Multiple rows with the same num belong to the same order.
* Parse the num column to determine the 'channel':
    * 'XXX-XXXXXXX' means Amazon
    * '3D-XXXX' means online store
    * 'AXXXX' means invoice
* Store the original order number in the database
* Store the channel type for analytics purposes
* Store the source name for identification of Amazon FBA orders
* Legacy Data Handling:
  * Pre-existing orders will be marked with:
    * order_number: 'LEGACY-' followed by the order ID
    * channel: 'legacy'
    * source: NULL

## Products
* Match products using the Item field from QB
* Extract the SKU from the parenthetical portion of the Item field
  * Example: From "01-6310.38K (SP10-38 asphalt anchors, carton of 6 anchors)", extract "01-6310.38K"
* If product doesn't exist, create it with:
  * SKU: Extracted value
  * Name: Full Item field
  * Description: Item Description field

## Error Handling
* Log all errors with detailed information
* Stop processing on critical errors:
  * Missing email for non-Amazon FBA orders
  * Invalid order number format
  * Unable to parse address
  * Unable to extract SKU from Item field
* Create error report with:
  * Row number in CSV
  * Error type
  * Relevant field values
  * Detailed error message

## Database Schema Changes

The following schema changes have been implemented:

### Orders Table
✓ Added columns:
* order_number TEXT NOT NULL - Stores QB order number (or 'LEGACY-{id}' for existing orders)
* channel TEXT NOT NULL - Stores sales channel (Amazon, online store, invoice, or 'legacy')
* source TEXT - Stores source name (used for identifying Amazon FBA orders)
* Created index on order_number for efficient lookups

## Implementation Phases

1. ✓ Database Migration (Completed)
   * Added new orders columns (order_number, channel, source)
   * Added index on order_number
   * Legacy orders marked with 'LEGACY-{id}' and channel 'legacy'
   
2. Parser Development
   * CSV parsing with error handling
   * Address parser
   * Order number parser
   * SKU extractor
   
3. Data Processing
   * Person matching/creation logic
   * Company matching/creation logic
   * Order creation with line items
   * Product matching/creation logic
   
4. Testing
   * Unit tests for all parsers
   * Integration tests for full import process
   * Test cases for error conditions
   * Test with various CSV formats and data conditions

5. Monitoring & Reporting
   * Error logging system
   * Import statistics
   * Data quality metrics
