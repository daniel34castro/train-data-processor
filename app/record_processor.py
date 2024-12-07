import datetime
import random

class RecordProcessor:
    def process(self, record):
        """
        Process a single train record: quality checks, standardization, and sanitization.
        
        Args:
            record (dict): The train record to process.
        
        Returns:
            dict: The cleaned and standardized record.
        
        Raises:
            ValueError: If critical fields are missing.
        """
        
        # Step 1: Data Quality Check
        critical_fields = ['trainNumber', 'departureDate', 'timeTableRows']
        for field in critical_fields:
            if field not in record or not record[field]:
                raise ValueError(f"Missing critical field: {field}")

        # Validate timeTableRows field
        if not isinstance(record['timeTableRows'], list):
            raise ValueError("timeTableRows should be a list")
        
        # Step 2: Data Standardization
        # Standardize date format to YYYY-MM-DD
        record['departureDate'] = self._standardize_date(record['departureDate'])
        # Sanitize string fields
        if 'operatorShortCode' in record:
            record['operatorShortCode'] = record['operatorShortCode'].strip()
            
        return record['timeTableRows']

    def _standardize_date(self, date_str):
        """Standardize a date string to the format YYYY-MM-DD."""
        try:
            date = datetime.datetime.fromisoformat(date_str)
            return date.strftime('%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}")

        
    def corrupt_record(self, record):        
        """
        Corrupt the record with a 5% chance by either removing trainNumber
        or corrupting the scheduledTime format. (pure function)

        Args:
            record (dict): The record to potentially corrupt.

        Returns:
            dict: A new record that may be corrupted, leaving the original unchanged.
        """
        # Create a copy of the record to avoid side effects
        corrupted_record = record.copy()

        # Introduce 10% chance to corrupt the record
        if random.random() < 0.1:
            corruption_type = random.choice(['remove_train_number', 'corrupt_scheduled_time'])

            if corruption_type == 'remove_train_number':
                if 'trainNumber' in corrupted_record:
                    # print("Corrupting record: Removing 'trainNumber'")
                    del corrupted_record['trainNumber']  # Remove trainNumber

            elif corruption_type == 'corrupt_scheduled_time':
                if 'timeTableRows' in corrupted_record:
                    row_to_corrupt = random.choice(corrupted_record['timeTableRows'])
                    if 'scheduledTime' in row_to_corrupt:
                        # print("Corrupting record: Corrupting 'scheduledTime'")
                        row_to_corrupt['scheduledTime'] = 'INVALID_DATE'  # Corrupt the scheduledTime

        return corrupted_record
