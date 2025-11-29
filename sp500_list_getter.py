import pandas as pd
import requests
import logging

logger = logging.getLogger()
logging.basicConfig(
    filename="logfile.log",
    format="[%(asctime)s][%(levelname)s] %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M"
    )

"""
Denna fil används för att skrapa S&P 500-listan direkt från slickcharts.com 
och fylla en DataFrame med bolagsnamn och symboler.
"""


class GetSp500List:
    def __init__(self) -> None:
        # Uppdaterad URL till Slickcharts S&P 500-sida
        self.url = "https://www.slickcharts.com/sp500"
        self.df = pd.DataFrame()
        
    def request_to_pd(self):
        """
        Hämtar HTML-tabellen från Slickcharts URL och konverterar den till en Pandas DataFrame.
        Använder 'requests' för att hämta innehållet och 'pandas.read_html' för att parsa tabellen.
        """
        try:
            logger.info(f"Försöker hämta data från {self.url}...")
            
            # Använd pandas.read_html för att automatiskt hitta och parsa tabeller.
            # Vi använder 'attrs' för att specificera den unika CSS-klassen på den önskade tabellen.
            df_list = pd.read_html(
                self.url,
                attrs={'class': 'table table-hover table-borderless table-sm'}
            )

            if not df_list:
                logger.error("Kunde inte hitta S&P 500-tabellen med den angivna CSS-klassen.")
                return

            # Den önskade tabellen är den första (och troligen enda) som matchar
            df = df_list[0]
            
            # Kolumnerna från Slickcharts är: '#', 'Company', 'Symbol', 'Weight', 'Price', 'Chg', '% Chg'
            # Vi väljer endast 'Company' och 'Symbol'.
            self.df = df[["#", "Company", "Symbol", "Weight"]].copy()
            self.df.set_index("#", inplace=True)            
            
            # Rename columns to match DB schema
            self.df = self.df.rename(columns={
                'Company': 'name', 
                'Symbol': 'ticker',
                'Weight': 'spy_weight'
            })
            
            # Clean ticker - replace . with -
            self.df["ticker"] = self.df["ticker"].str.replace(".", "-", regex=False)
            
            # Add ETF info
            self.df["etfs"] = "SPY"
            # Store weight as JSON for future expansion
            self.df["etf_weights"] = self.df["spy_weight"].apply(lambda x: f'{{"SPY": {x}}}')
            
            # Add sector and industry as None (will be populated later if needed)
            self.df["sector"] = None
            self.df["industry"] = None
            
            # Keep only relevant columns for DB
            self.df = self.df[["ticker", "name", "sector", "industry", "etfs", "etf_weights"]]
            
            logger.info(f"Hämtade framgångsrikt {len(self.df)} bolagssymboler.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Kunde inte ansluta till Slickcharts ({self.url}). Kontrollera din anslutning: {e}")
        except Exception as e:
            logger.error(f"Ett oväntat fel inträffade under HTML-skrapning: {e}")
    
    def get_earnings_date(self):
        pass

    def update_db(self):
        from access_db import DBAccess
        db = DBAccess()
        try:
            db.upsert_companies(self.df)
            print("S&P 500 list updated in database.")
        except Exception as e:
            print(f"Failed to update database: {e}")
        finally:
            db.close_connection()
            
    def get_df(self):
        """Returnerar den skrapade DataFramen."""
        return self.df


# Exempel på körning för att testa skriptet:
if __name__ == "__main__":
    sp500_scraper = GetSp500List()
    
    # 1. Hämta S&P 500-listan genom HTML-skrapning
    sp500_scraper.request_to_pd()
    
    # 2. Skriv ut resultatet (de första 10 raderna)
    final_df = sp500_scraper.get_df()
    
    if not final_df.empty:
        print("\n--- SLUTRESULTAT (FÖRSTA 10 RADER) ---")
        print(final_df.head(10))
    else:
        print("\n--- Kunde inte hämta data. Kontrollera loggfilen (logfile.log) för fel. ---")