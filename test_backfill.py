#!/usr/bin/env python3
"""
Test script for the backfill functionality.
Run this to test the backfill without starting the full bot.
"""

import asyncio
import sys
import os
import logging
from datetime import datetime
import datetime as dt

# Add the bot directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import generate_backfill_months, get_month_range, fetch_total_wager, fetch_weighted_wager
from db import backfill_monthly_totals_for_date, get_monthly_totals

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_backfill():
    """Test the backfill functionality"""
    print("=== Testing Backfill Functionality ===")
    
    # Test month generation
    print("\n1. Testing month generation:")
    months = generate_backfill_months(2025, 6)  # Start from June 2025
    print(f"Months to backfill: {months}")
    
    # Test date range generation
    print("\n2. Testing date range generation:")
    for year, month in months[:3]:  # Test first 3 months
        start_date, end_date = get_month_range(year, month)
        print(f"{year}-{month:02d}: {start_date} to {end_date}")
    
    # Test getting existing data
    print("\n3. Current monthly totals in database:")
    existing_data = get_monthly_totals()
    if existing_data:
        for data in existing_data:
            print(f"  {data['year']}-{data['month']:02d}: Total=${data['total_wager']:,.2f}, Weighted=${data['weighted_wager']:,.2f}")
    else:
        print("  No existing data found")
    
    # Test API fetch for one month (July 2025)
    print("\n4. Testing API fetch for July 2025:")
    try:
        start_date, end_date = get_month_range(2025, 7)
        print(f"Fetching data for July 2025: {start_date} to {end_date}")
        
        total_wager_data = await asyncio.to_thread(fetch_total_wager, start_date, end_date)
        weighted_wager_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
        
        total_wager = sum(
            entry.get("wagered", 0)
            for entry in total_wager_data
            if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
        )
        
        weighted_wager = sum(
            entry.get("weightedWagered", 0)
            for entry in weighted_wager_data
            if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
        )
        
        print(f"  Total Wager: ${total_wager:,.2f}")
        print(f"  Weighted Wager: ${weighted_wager:,.2f}")
        print(f"  Total entries: {len(total_wager_data)} total, {len(weighted_wager_data)} weighted")
        
        # Test saving to database
        print("\n5. Testing database save:")
        success = backfill_monthly_totals_for_date(2025, 7, total_wager, weighted_wager)
        print(f"  Save result: {'Success' if success else 'Failed or already exists'}")
        
    except Exception as e:
        print(f"  Error during API test: {e}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    asyncio.run(test_backfill())
