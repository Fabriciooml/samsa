from prometheus_client import Counter, Histogram

DELIVERIES = Counter(
    "telegram_kafka_deliveries_total", "Delivery outcomes", ["outcome"]
)
SEND_LATENCY = Histogram(
    "telegram_kafka_send_seconds", "Time spent sending through Telegram"
)
RETRIES = Counter("telegram_kafka_retries_total", "Telegram send retries", ["reason"])
