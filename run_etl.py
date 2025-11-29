"""
Main ELT Pipeline Orchestrator
Runs the complete data pipeline: Extract from APIs -> Load to Database -> Transform in DB
"""
import logging
from datetime import datetime
from api import API
from access_db import DBAccess
from sp500_list_getter import GetSp500List

# Setup logging
logging.basicConfig(
    filename=f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    format="[%(asctime)s][%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M"
)

logger = logging.getLogger()

def run_sp500_update():
    """Step 1: Update S&P 500 company list with ETF weights"""
    logger.info("=" * 60)
    logger.info("STEP 1: Updating S&P 500 Company List")
    logger.info("=" * 60)
    
    try:
        sp500 = GetSp500List()
        sp500.request_to_pd()
        df = sp500.get_df()
        
        if df.empty:
            logger.error("Failed to fetch S&P 500 list")
            return False
        
        logger.info(f"Fetched {len(df)} companies from S&P 500")
        sp500.update_db()
        logger.info("S&P 500 list updated successfully")
        return True
        
    except Exception as e:
        logger.error(f"S&P 500 update failed: {e}")
        return False

def run_financials_update(batch_size=50):
    """Step 2: Fetch and load financial statements for all companies"""
    logger.info("=" * 60)
    logger.info("STEP 2: Fetching Financial Statements")
    logger.info("=" * 60)
    
    try:
        # Get list of companies that need updates
        db = DBAccess()
        companies_to_update = db.get_list_of_needed_updates()
        
        if not companies_to_update:
            logger.info("No companies need updates")
            return True
        
        logger.info(f"Found {len(companies_to_update)} companies to update")
        
        # Process in batches
        total_companies = len(companies_to_update)
        for i in range(0, total_companies, batch_size):
            batch = companies_to_update[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_companies + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")
            
            # Fetch financials
            api = API()
            api.add_list(batch)
            api.get_financials()
            
            data, errors, timestamp = api.get_all()
            
            # Load to database
            if data:
                failed = db.insert_financial_data(data)
                if failed:
                    logger.warning(f"Failed to insert data for: {failed}")
                
                # Update last_update timestamp
                successful = [s for s in batch if s not in failed]
                if successful:
                    db.update_last_update(timestamp, successful)
                
                logger.info(f"Batch {batch_num}: Inserted {len(data)} dataframes, {len(successful)} companies successful")
            
            if errors:
                logger.warning(f"Batch {batch_num}: Errors for {len(errors)} companies: {errors}")
        
        db.close_connection()
        logger.info("Financial statements update completed")
        return True
        
    except Exception as e:
        logger.error(f"Financials update failed: {e}")
        return False

def run_daily_prices_update():
    """Step 3: Fetch and load daily price data"""
    logger.info("=" * 60)
    logger.info("STEP 3: Updating Daily Prices")
    logger.info("=" * 60)
    
    try:
        db = DBAccess()
        # Get all active companies (not just those needing financial updates)
        companies = db.get_all_active_companies()
        
        if not companies:
            logger.info("No companies to update prices for")
            return True
        
        logger.info(f"Fetching prices for {len(companies)} companies")
        
        api = API()
        api.add_list(companies)
        prices = api.get_daily_prices()
        
        if prices:
            db.insert_daily_prices(prices)
            logger.info(f"Inserted price data for {len(prices)} companies")
        
        db.close_connection()
        return True
        
    except Exception as e:
        logger.error(f"Daily prices update failed: {e}")
        return False

def run_full_pipeline():
    """Run the complete ELT pipeline"""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"STARTING FULL ELT PIPELINE - {start_time}")
    logger.info("=" * 60)
    
    results = {
        "sp500_update": run_sp500_update(),
        "financials_update": run_financials_update(),
        "prices_update": run_daily_prices_update()
    }
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"S&P 500 Update: {'SUCCESS' if results['sp500_update'] else 'FAILED'}")
    logger.info(f"Financials Update: {'SUCCESS' if results['financials_update'] else 'FAILED'}")
    logger.info(f"Prices Update: {'SUCCESS' if results['prices_update'] else 'FAILED'}")
    logger.info(f"Total Duration: {duration}")
    logger.info("=" * 60)
    
    return all(results.values())

if __name__ == "__main__":
    success = run_full_pipeline()
    exit(0 if success else 1)
