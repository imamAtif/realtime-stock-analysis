import json
import os

from kafka import KafkaProducer
from alpaca_trade_api import REST
import time


endpoint = "https://paper-api.alpaca.markets"
import dotenv

dotenv.load_dotenv()
API_KEY = os.getenv("ALPACA_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
print(KAFKA_BROKER)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
API_SECRET = os.getenv("ALPACA_SECRET")
STOCK_SYMBOL = "AAPL"
SSL_CA_FILE_PATH = "ca-certificate.crt"
api = REST(API_KEY, API_SECRET, endpoint)
# kafka_producer = KafkaProducer(
#     bootstrap_servers = KAFKA_BROKER,
#     security_protocol ='SSL',
#     ssl_cafile='ca-certificate.crt',
#     ssl_certfile ='user-access-certificate.crt',
#     ssl_keyfile = 'user-access-key.key'
# )
SASL_MECHANISM = "SCRAM-SHA-256"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # ✅ Serializes JSON and encodes as bytes

)

print(producer)
while True:
    quote = api.get_latest_quote(STOCK_SYMBOL)
    trade = api.get_latest_trade(STOCK_SYMBOL)
    # print(quote)
    # print(quote,trade)
    # print(f"Time: {trade.t}")
    # print(f"Bid Price: {quote.bp}, Ask Price: {quote.ap}")
    # print(f"Last Trade Price: {trade.p}, Trade Volume: {trade.s}")
    message = {
        "timestamp": str(trade.t),
        "symbol": STOCK_SYMBOL,
        "bid_price": quote.bp,
        "ask_price": quote.ap,
        "trade_price": trade.p,
        "trade_volume": trade.s,
    }
    producer.send(KAFKA_TOPIC, message)

    print(message)
    print("message sent to kafka")
    time.sleep(15)
