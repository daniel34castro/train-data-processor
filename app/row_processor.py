import datetime


class RowProcessor:
    def __init__(self):
        self.processed_keys = set()  # To track duplicates based on unique identifiers

    def process(self, row, record):
        self._flat_row(row, record)
        self._standardize_timetable_row(row)
        
        
        self._createKey(row)
        return row
    
    def _standardize_timetable_row(self, row):
        """Standardize and sanitize individual timetable row entries."""
        # Convert scheduledTime to ISO format and remove time zone info
        if 'scheduledTime' in row:
            row['scheduledTime'] = self._standardize_datetime(row['scheduledTime'])
        else:
            raise ValueError(f"scheduledTime does not exist: {row}")

        # Sanitize stationShortCode
        if 'stationShortCode' in row:
            row['stationShortCode'] = row['stationShortCode'].strip()

        # Ensure differenceInMinutes is an integer
        if 'differenceInMinutes' in row:
            row['differenceInMinutes'] = int(row['differenceInMinutes']) if row['differenceInMinutes'] else 0


    def _standardize_datetime(self, datetime_str):
        """
        Standardize datetime to the format 'YYYY-MM-DDTHH:MM:SS.sssZ'.
        Raises an exception if it cannot parse the input.

        Args:
            datetime_str (str): The datetime string to standardize.

        Returns:
            str: The standardized datetime string.

        Raises:
            ValueError: If the input cannot be parsed into the target format.
        """
        try:
            # Try to parse the input string into a datetime object
            dt = datetime.datetime.fromisoformat(datetime_str.rstrip('Z'))  # Remove 'Z' for compatibility
            # Format back to 'YYYY-MM-DDTHH:MM:SS.sssZ'
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        except Exception:
            raise ValueError("Invalid date", f"Invalid datetime format: {datetime_str}")
    
    def _flat_row(self, row, record):
        """
        Add train information from the record to each row.

        Args:
            row (dict): The timetable row to update.
            record (dict): The record containing train-level information.
        """
        # Define the list of fields to copy from record to row
        fields_to_copy = [
            "trainCategory", "trainType", "version", "runningCurrently",
            "timetableAcceptanceDate", "commuterLineID", "timetableType",
            "operatorUICCode", "cancelled", "trainNumber", "departureDate",
            "operatorShortCode"
        ]
        
        # Copy each field from record to row
        for field in fields_to_copy:
            if field in record:  # Ensure the field exists in the record
                row[field] = record[field]

    def _createKey(self, row):
        # Check for duplicate records using a composite key (trainNumber + departureDate)
        unique_key = f"{row['trainNumber']}_{row['scheduledTime']}"
        if unique_key in self.processed_keys:
            raise ValueError("Duplicate record", f"Duplicate record detected: {unique_key}")
        self.processed_keys.add(unique_key)
        row['uniqueKey'] = unique_key