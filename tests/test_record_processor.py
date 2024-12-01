import unittest

from app.record_processor import RecordProcessor
from app.row_processor import RowProcessor

class TestRecordProcessor(unittest.TestCase):
    def setUp(self):
        """Set up any preconditions or shared objects for the tests."""
        self.record_processor = RecordProcessor()
        self.row_processor = RowProcessor()
        
    def test_standardize_datetime_valid_input(self):
        # Test input already in correct format
        result = self.row_processor._standardize_datetime('2024-11-28T08:19:00.000Z')
        self.assertEqual(result, '2024-11-28T08:19:00.000Z')

        # Test input in incorrect format (no 'T' separator and no 'Z')
        result = self.row_processor._standardize_datetime('2024-11-28 08:19:00')
        self.assertEqual(result, '2024-11-28T08:19:00.000Z')
    
    def test_standardize_datetime_invalid_input(self):
        # Test invalid input
        with self.assertRaises(ValueError):
            self.row_processor._standardize_datetime('Invalid-Date')
        
    def test_process_missing_critical_fields(self):
        record = {
            "departureDate": "2024-11-28",
            "timeTableRows": []
        }
        with self.assertRaises(ValueError) as context:
            self.record_processor.process(record)
        self.assertIn("Missing critical field", str(context.exception))