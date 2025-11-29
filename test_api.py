from api import API
import logging

# Setup logging to console
logging.basicConfig(level=logging.INFO)

def test_api():
    print("Testing API...")
    api = API()
    
    # Test add_list
    api.add_list(["AAPL"])
    print("Added AAPL to list.")
    
    # Test get_financials
    print("Fetching financials...")
    api.get_financials()
    data, errors, time = api.get_all()
    
    print(f"Fetched {len(data)} dataframes.")
    if data:
        print(f"First dataframe shape: {data[0].shape}")
        print(f"First dataframe columns: {data[0].columns}")
    
    if errors:
        print(f"Errors: {errors}")
        
    # Test get_daily_prices
    print("Fetching daily prices...")
    prices = api.get_daily_prices()
    print(f"Fetched {len(prices)} price dataframes.")
    if prices:
        print(f"First price dataframe shape: {prices[0].shape}")

    # Test get_earnings_date
    print("Fetching earnings date...")
    earnings = api.get_earnings_date()
    print("Earnings date dataframe:")
    print(earnings)

if __name__ == "__main__":
    test_api()
