import yfinance as yf
import datetime as dt
import pandas as pd
from time import sleep
import logging

logger = logging.getLogger()

class API:
    def __init__(self) -> None:
        """
        Initializer
        """
        self.list_of_companies = []
        self.list = []
        self.list_of_error = []
        self.time = dt.datetime.now()
        
    def add_list(self, list_of_companies):
        """
        Add and/or resets the attribute List_of_companies
        aswell as resets the List attributes.
        """
        self.list_of_companies = list_of_companies
        self.list = []
        self.list_of_error = []
        
    def get_financials(self):
        """
        Downloads the financials statements from the given companies in both annual and quarterly format.
        Formats data vertically: ticker, fiscal_date, field_name, field_value, unit, source, period_type, created_at
        """
        if not self.list_of_companies:
            logging.info("self.list_of_companies is empty")
            print("list is empty, provide list with .add_list()")
            return

        for symbol in self.list_of_companies:
            try:
                ticker = yf.Ticker(symbol)
                
                # Helper to process dataframe
                def process_df(df, table_name, period_type):
                    if not df.empty:
                        # yfinance returns dates as columns. We need to melt.
                        # df index is field names (e.g. 'Total Revenue')
                        # df columns are dates
                        
                        # Reset index to make field names a column
                        df = df.reset_index()
                        df = df.rename(columns={"index": "field_name"})
                        
                        # Melt
                        df_melted = df.melt(id_vars=["field_name"], var_name="fiscal_date", value_name="field_value")
                        
                        # Add metadata columns
                        df_melted["ticker"] = symbol
                        df_melted["unit"] = "currency" # Default, maybe refine later
                        df_melted["source"] = "yfinance"
                        df_melted["period_type"] = period_type
                        df_melted["created_at"] = dt.datetime.now()
                        df_melted["table_name"] = table_name
                        
                        # Ensure fiscal_date is date
                        df_melted["fiscal_date"] = pd.to_datetime(df_melted["fiscal_date"]).dt.date
                        
                        self.list.append(df_melted)
                    else:
                        logging.warning(f"{symbol} {period_type} {table_name} DF is empty")

                # Fetch Statements (Annual)
                process_df(ticker.get_income_stmt(freq="yearly"), "income_statement", "Annual")
                process_df(ticker.get_balancesheet(freq="yearly"), "balance_sheet", "Annual")
                process_df(ticker.get_cash_flow(freq="yearly"), "cashflow_statement", "Annual")
                
                # Fetch Statements (Quarterly)
                process_df(ticker.get_income_stmt(freq="quarterly"), "income_statement", "Quarterly")
                process_df(ticker.get_balancesheet(freq="quarterly"), "balance_sheet", "Quarterly")
                process_df(ticker.get_cash_flow(freq="quarterly"), "cashflow_statement", "Quarterly")
                
            except Exception as e:
                logging.error(f"Failed to fetch data for {symbol}: {e}")
                self.list_of_error.append(symbol)
            
            sleep(0.1)
        
        self.time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_daily_prices(self):
        """
        Fetch daily prices for the companies.
        Returns a list of DataFrames formatted for database insertion.
        """
        price_list = []
        for symbol in self.list_of_companies:
            try:
                # Fetch all historical data
                df = yf.download(symbol, period="max", interval="1d", progress=False)
                
                if not df.empty:
                    # Flatten multi-level columns if present
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    
                    # Reset index to make Date a column
                    df = df.reset_index()
                    
                    # Rename columns to match database schema
                    df = df.rename(columns={
                        'Date': 'trade_date',
                        'Open': 'open_price',
                        'Close': 'close_price',
                        'High': 'high_price',
                        'Low': 'low_price',
                        'Volume': 'volume'
                    })
                    
                    # Add ticker column
                    df['ticker'] = symbol
                    
                    # Select only the columns we need
                    df = df[['ticker', 'trade_date', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']]
                    
                    # Add created_at timestamp
                    df['created_at'] = dt.datetime.now()
                    
                    price_list.append(df)
                else:
                    logging.warning(f"No price data for {symbol}")
            except Exception as e:
                logging.error(f"Failed to fetch prices for {symbol}: {e}")
        return price_list

    def get_earnings_date(self): 
        """
        download the earnings date for the given companies.
        """
        df = pd.DataFrame(self.list_of_companies, columns=["Symbol"])
        df["Earnings date"] = None
        
        for i, symbol in enumerate(self.list_of_companies):
            try:
                ticker = yf.Ticker(symbol)
                if len(ticker.calendar.get("Earnings Date", [])) >= 1:
                    earning_date = ticker.calendar["Earnings Date"][0]
                    df.at[i, "Earnings date"] = earning_date
                else:
                    logger.warning(f"No earnings date found for {symbol}")
            except Exception as e:
                logging.warning(f"Error getting earnings for {symbol}: {e}")
                
        return df
            
    def get_list_of_df(self):
        return self.list
    
    def get_list_of_failed_calls(self):
        return self.list_of_error
        
    def get_timestamp_of_calls(self):
        return self.time
    
    def get_all(self):
        return self.list, self.list_of_error, self.time