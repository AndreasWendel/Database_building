from access_db import DBAccess
from api import API
import logging
import os

# Setup logging
logger = logging.getLogger()
logging.basicConfig(
    filename="Main_logfile.log",
    format="[%(asctime)s][%(levelname)s] %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M"
)

logger.info("Script started")

# Database connection details from env or defaults
DB_SERVER = os.getenv("DB_SERVER", "localhost")
DB_NAME = os.getenv("DB_NAME", "YourDatabase")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
TRUSTED = os.getenv("DB_TRUSTED", "yes")

try:
    dbaccess = DBAccess(db_server=DB_SERVER, db_name=DB_NAME, driver=DB_DRIVER, trusted=TRUSTED)
    api = API()

    # 1. Check for needed updates
    update_list = dbaccess.get_list_of_needed_updates()
    
    if not update_list:
        logging.info("No financial updates needed.")
        
        # Check for earnings date updates
        earnings_update_list = dbaccess.check_earnings_last_update()
        if earnings_update_list:
            logging.info(f"Updating earnings dates for: {earnings_update_list}")
            api.add_list(earnings_update_list)
            earnings_df = api.get_earnings_date()
            dbaccess.update_earnings_date(earnings_df)
        
        dbaccess.close_connection()
        exit()
    else:
        logging.info(f"Update list include: {', '.join(update_list)}")

    # 2. Fetch Financials
    api.add_list(update_list)
    api.get_financials()
    
    financials, failed_calls, time = api.get_all()
    
    if failed_calls:
        for symbol in failed_calls:
            logging.warning(f"Stock {symbol} failed to download data")

    # 3. Insert Financials
    failed_update = dbaccess.insert_financial_data(financials)
    
    # 4. Fetch and Insert Daily Prices
    # Note: This might be heavy if update_list is large. 
    # Consider if this should be always run or only on specific conditions.
    # For now, we run it for the companies being updated.
    prices = api.get_daily_prices()
    dbaccess.insert_daily_prices(prices)

    # 5. Update Metadata (Last Update Time)
    # Filter out failed updates
    updated_symbols = list(set(update_list) - set(failed_update))
    dbaccess.update_last_update(time, updated_symbols)

    # 6. Update Earnings Dates for updated companies
    # (Since we just got fresh data, might as well check next earnings)
    api.add_list(updated_symbols)
    earnings_df = api.get_earnings_date()
    dbaccess.update_earnings_date(earnings_df)

    dbaccess.close_connection()
    logger.info("Script finished successfully")

except Exception as e:
    logging.exception("An error occurred in the main execution")
    print(f"Error: {e}")
