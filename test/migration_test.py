import unittest
from datetime import datetime, timedelta
from bdms.utils import (
    generate_daily_date_range, 
    generate_monthly_date_range, 
    split_date_range
)

def split_date_range_legacy(start_date, end_date):
    """Legacy function to split the date range into monthly and daily dates."""
    # Convert the start and end dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")
 
    # Initialize the monthly and daily date lists
    monthly_dates = []
    daily_dates = []

    # Start at the beginning of the first month
    current_date = start_date.replace(day=1)
    
    # Add all full months
    while current_date < end_date.replace(day=1):
        monthly_dates.append(current_date)
        current_date = (current_date + timedelta(days=32)).replace(day=1)
        
    # Reset the current date if there are no monthly dates. This indicates that
    # the date range is less than a month.
    if not monthly_dates:
        current_date = start_date
        
    # Add remaining daily dates
    while current_date < end_date:
        daily_dates.append(current_date)
        current_date += timedelta(days=1)
    
    return monthly_dates, daily_dates


test_cases = [
    ("2024-01-01", "2024-02-01"), 
    ("2024-01-01", "2024-01-02"), 
    ("2023-01-01", "2024-01-01"), 
    ("2021-01-13", "2021-03-25"), 
    ("2021-01-13", "2021-03-31"),
]

class TestMigration(unittest.TestCase):
    def test_split_date_range(self):
        for start_date, end_date in test_cases:
        
            monthly_dates0, daily_dates0 = split_date_range(
                start_date, end_date
            )
            monthly_dates, daily_dates = split_date_range_legacy(
                start_date, end_date
            )
            
            self.assertEqual(len(monthly_dates), len(monthly_dates0))
            self.assertEqual(len(daily_dates), len(daily_dates0))
            
            for i, date in enumerate(monthly_dates + daily_dates):
                if i < len(monthly_dates):
                    self.assertEqual(date, monthly_dates0[i])
                else:
                    self.assertEqual(date, daily_dates0[i - len(monthly_dates)])
        
if __name__ == '__main__':
    unittest.main()