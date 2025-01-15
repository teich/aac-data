#!/usr/bin/env python3
import unittest
from qb_sales_parser import QBSalesParser, Address, QBSalesRecord, AddressParser

class TestQBSalesParser(unittest.TestCase):
    def setUp(self):
        self.parser = QBSalesParser('inputs/test_sales.csv')
        
    def test_address_parser(self):
        """Test address parsing functionality."""
        test_cases = [
            (
                "1 Anystreet, Ri 02816-7613 US",
                Address(
                    street="1 Anystreet",
                    city="",
                    state="Ri",
                    zip_code="02816",
                    country="US"
                )
            ),
            (
                "4 Anystreet, NY 10001 US",
                Address(
                    street="4 Anystreet",
                    city="",
                    state="NY",
                    zip_code="10001",
                    country="US"
                )
            )
        ]
        
        for address_str, expected in test_cases:
            result = self.parser.address_parser.parse(address_str)
            self.assertEqual(result, expected)
            
    def test_channel_detection(self):
        """Test sales channel detection from order numbers."""
        test_cases = [
            ("610-4148257", "amazon"),
            ("912-3712214", "amazon"),
            ("3D-1234", "online_store"),
            ("A1234", "invoice")
        ]
        
        for order_num, expected_channel in test_cases:
            record = QBSalesRecord(
                type="Sales Receipt",
                date="11/25/2016",
                order_number=order_num,
                source_name="Test",
                address_raw="1 Test St, NY 10001 US",
                contact_name="Test",
                phone="1234567890",
                email="test@example.com",
                memo="Test",
                name="Test",
                item="01-6310.38K (Test Item)",
                item_description="Test Item",
                quantity=1,
                price=42.00,
                amount=42.00
            )
            self.assertEqual(record.channel, expected_channel)
            
    def test_sku_extraction(self):
        """Test SKU extraction from item field."""
        test_cases = [
            (
                "01-6310.38K (SP10-38 asphalt anchors, carton of 6 anchors)",
                "01-6310.38K"
            ),
            (
                "ABC-123 (Some product description)",
                "ABC-123"
            )
        ]
        
        for item, expected_sku in test_cases:
            record = QBSalesRecord(
                type="Sales Receipt",
                date="11/25/2016",
                order_number="610-4148257",
                source_name="Test",
                address_raw="1 Test St, NY 10001 US",
                contact_name="Test",
                phone="1234567890",
                email="test@example.com",
                memo="Test",
                name="Test",
                item=item,
                item_description="Test Item",
                quantity=1,
                price=42.00,
                amount=42.00
            )
            self.assertEqual(record.sku, expected_sku)
            
    def test_full_parse(self):
        """Test parsing the entire test CSV file."""
        records = self.parser.run()
        
        # Verify we got all 5 records
        self.assertEqual(len(records), 5)
        
        # Check specific records
        amazon_record = next(r for r in records if r.order_number == "610-4148257")
        self.assertEqual(amazon_record.channel, "amazon")
        self.assertEqual(amazon_record.sku, "01-6310.38K")
        
        online_store_record = next(r for r in records if r.order_number == "3D-1234")
        self.assertEqual(online_store_record.channel, "online_store")
        self.assertEqual(online_store_record.quantity, 2)
        
        invoice_record = next(r for r in records if r.order_number == "A1234")
        self.assertEqual(invoice_record.channel, "invoice")
        self.assertEqual(invoice_record.amount, 126.00)
        
        fba_record = next(r for r in records if r.source_name == "Amazon FBA")
        self.assertEqual(fba_record.channel, "amazon")
        self.assertTrue(not fba_record.email)  # FBA record has no email
        
if __name__ == '__main__':
    unittest.main()
