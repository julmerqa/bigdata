import csv
import json
import os
import time

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC_CRYPTO",
    "crypto-btc-eventos"
)

INTERVAL_MS = int(
    os.getenv("CRYPTO_INTERVAL_MS", "3000")
)

CSV_FILE = os.getenv(
    "CRYPTO_CSV",
    "/data/BTCUSDT-1m-2024-12.csv"
)

SYMBOL = "BTCUSDT"


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


print("=" * 60)
print("PRODUCTOR CRYPTO BTC")
print("=" * 60)
print("Broker :", BOOTSTRAP_SERVERS)
print("Topic  :", TOPIC)
print("Archivo:", CSV_FILE)
print("Par    :", SYMBOL)
print("=" * 60)


with open(CSV_FILE, "r", encoding="utf-8") as archivo:

    lector = csv.reader(archivo)

    for fila in lector:

        evento = {
            "tipoEvento": "crypto.candle",
            "symbol": SYMBOL,
            "timestamp": int(time.time() * 1000),
            "marketOpenTime": int(fila[0]),
            "open": float(fila[1]),
            "high": float(fila[2]),
            "low": float(fila[3]),
            "close": float(fila[4]),
            "volume": float(fila[5]),
            "quote_volume": float(fila[7]),
            "trades": int(fila[8]),
            "taker_buy_volume": float(fila[9]),
            "origen": "binance-public-data-replay"
        }

        producer.send(
            TOPIC,
            key=SYMBOL,
            value=evento
        ).get(timeout=10)

        print(
            json.dumps({
                "status": "published",
                "symbol": SYMBOL,
                "close": evento["close"],
                "volume": evento["volume"],
                "trades": evento["trades"]
            })
        )

        time.sleep(INTERVAL_MS / 1000)