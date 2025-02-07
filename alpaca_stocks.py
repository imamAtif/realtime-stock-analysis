import os
from shlex import quote

from alpaca_trade_api import REST
import time


endpoint = "https://paper-api.alpaca.markets"
import dotenv

dotenv.load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY")
API_SECRET = os.getenv("ALPACA_SECRET")
STOCK_SYMBOL = "AAPL"

api = REST(API_KEY,API_SECRET,endpoint)
while True:
    quote = api.get_latest_quote(STOCK_SYMBOL)
    trade = api.get_latest_trade(STOCK_SYMBOL)
    # print(quote)
    # print(quote,trade)
    print(f"Time: {trade.t}")
    print(f"Bid Price: {quote.bp}, Ask Price: {quote.ap}")
    print(f"Last Trade Price: {trade.p}, Trade Volume: {trade.s}")
    time.sleep(5)

