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
        Downloads the financials statements from the given companies in quarterly format.
        """
        if not self.list_of_companies:
            logging.info("self.list_of_companies is empty")
            print("list is empty, provide list with .add_list()")
            return

        for symbol in self.list_of_companies:
            try:
                ticker = yf.Ticker(symbol)
                
                # Helper to process dataframe
                def process_df(df, type_name):
                    if not df.empty:
                        df = df.T.iloc[::-1]
                        df["Symbol"] = symbol
                        df["StatementType"] = type_name
                        self.list.append(df)
                    else:
                        logging.warning(f"{symbol} Quarterly {type_name} DF is empty")

                # Fetch Statements
                process_df(ticker.quarterly_income_stmt, "Income Statement")
                process_df(ticker.quarterly_balance_sheet, "Balance Sheet")
                process_df(ticker.quarterly_cashflow, "Cash Flow")
                
            except Exception as e:
                logging.error(f"Failed to fetch data for {symbol}: {e}")
                self.list_of_error.append(symbol)
            
            sleep(0.1)
        
        self.time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_daily_prices(self):
        """
        Fetch daily prices for the companies.
        Returns a list of DataFrames.
        """
        price_list = []
        for symbol in self.list_of_companies:
            try:
                # Fetch last 2 years of data
                df = yf.download(symbol, period="2y", interval="1d", progress=False)
                if not df.empty:
                    df["Symbol"] = symbol
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