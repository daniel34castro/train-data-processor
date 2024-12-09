import unittest
import random
from copy import deepcopy

from app.record_processor import RecordProcessor


class TestRecordProcessor(unittest.TestCase):
    def setUp(self):
        """Set up a valid train record."""
        self.processor = RecordProcessor()
        self.valid_record = {
            "trainNumber": 265,
            "departureDate": "2024-11-28",
            "operatorUICCode": 10,
            "operatorShortCode": " vr ",
            "trainType": "PYO",
            "trainCategory": "Long-distance",
            "commuterLineID": "",
            "runningCurrently": False,
            "cancelled": False,
            "version": 289877099218,
            "timetableType": "REGULAR",
            "timetableAcceptanceDate": "2024-05-08T10:55:08.000Z",
            "timeTableRows": [
                {
                    "stationShortCode": "HKI",
                    "stationUICCode": 1,
                    "countryCode": "FI",
                    "type": "DEPARTURE",
                    "trainStopping": True,
                    "commercialStop": True,
                    "commercialTrack": "8",
                    "cancelled": False,
                    "scheduledTime": "2024-11-28T17:29:00.000Z",
                    "actualTime": "2024-11-28T17:30:27.000Z",
                    "differenceInMinutes": 1,
                    "causes": [],
                    "trainReady": {
                        "source": "KUPLA",
                        "accepted": True,
                        "timestamp": "2024-11-28T17:09:28.000Z"
                    }
                },
                {
                    "stationShortCode": "PSL",
                    "stationUICCode": 10,
                    "countryCode": "FI",
                    "type": "ARRIVAL",
                    "trainStopping": True,
                    "commercialStop": True,
                    "commercialTrack": "6",
                    "cancelled": False,
                    "scheduledTime": "2024-11-28T17:34:00.000Z",
                    "actualTime": "2024-11-28T17:36:19.000Z",
                    "differenceInMinutes": 2,
                    "causes": []
                }
            ]
        }

    ## --- process method tests ---

    def test_process_valid_record(self):
        """Test processing a valid train record."""
        record = deepcopy(self.valid_record)
        result = self.processor.process(record)
        self.assertEqual(record["departureDate"], "2024-11-28")
        self.assertEqual(record["operatorShortCode"], "vr")
        self.assertIsInstance(result, list)
        self.assertEqual(result, record["timeTableRows"])

    def test_process_missing_critical_field(self):
        """Test processing a record missing critical fields raises ValueError."""
        critical_fields = ['trainNumber', 'departureDate', 'timeTableRows']
        for field in critical_fields:
            with self.subTest(field=field):
                record = deepcopy(self.valid_record)
                del record[field]
                with self.assertRaises(ValueError) as context:
                    self.processor.process(record)
                self.assertIn(f"Missing critical field: {field}", str(context.exception))

    def test_process_invalid_timeTableRows_type(self):
        """Test that a non-list timeTableRows raises ValueError."""
        record = deepcopy(self.valid_record)
        record['timeTableRows'] = "invalid_list"
        with self.assertRaises(ValueError) as context:
            self.processor.process(record)
        self.assertIn("timeTableRows should be a list", str(context.exception))

    def test_process_invalid_date_format(self):
        """Test that an invalid departureDate raises ValueError."""
        record = deepcopy(self.valid_record)
        record['departureDate'] = "invalid_date"
        with self.assertRaises(ValueError) as context:
            self.processor.process(record)
        self.assertIn("Invalid date format", str(context.exception))



if __name__ == "__main__":
    unittest.main()