import yfinance as yf

try:
    print("Downloading AAPL...")
    data = yf.download("AAPL", period="1mo")
    print(data.head())
    print(data.info())
    print(data["Date"].min())
    print(data["Date"].max())
    print("Fetching Ticker...")
    ticker = yf.Ticker("AAPL")
    print(ticker.info.get('symbol'))
except Exception as e:
    print(e)
