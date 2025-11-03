from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from loguru import logger
import json
import time
import random
from datetime import datetime, timedelta, timezone
from itertools import cycle
import sys
import os

# ------------------------------
# 🔧 Logging Configuration
# ------------------------------
logger.remove()  # Remove default handler
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

logger.add(
    "logs/producer.log",
    rotation="5 MB",
    retention=10,
    compression="zip",
    enqueue=True,
    level="INFO",
)

# ------------------------------
# ⚙️ Kafka Configuration
# ------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")
TOPIC = "sensor_data"

MAX_RETRIES = 10
RETRY_DELAY = 5  # seconds

producer = None
for attempt in range(1, MAX_RETRIES + 1):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=1,  # send almost immediately
            batch_size=16 * 1024,  # smaller batch for smoother streaming
            compression_type="lz4",
            acks=1,  # ensure leader ack for reliability
        )
        logger.success(
            "✅ KafkaProducer initialized successfully and connected to broker!"
        )
        break
    except NoBrokersAvailable:
        logger.warning(
            f"Kafka broker not available (attempt {attempt}/{MAX_RETRIES}). Retrying in {RETRY_DELAY}s..."
        )
        time.sleep(RETRY_DELAY)
    except Exception as e:
        logger.exception(f"Failed to initialize KafkaProducer: {e}")
        time.sleep(RETRY_DELAY)

if not producer:
    logger.error("❌ Could not connect to Kafka after multiple retries. Exiting.")
    raise SystemExit(1)

# ------------------------------
# 🧩 Data Generation Setup
# ------------------------------
MAC_IDS = [f"MAC_{i:02d}" for i in range(1, 11)]
IST = timezone(timedelta(hours=5, minutes=30))
mac_cycle = cycle(MAC_IDS)

TARGET_RATE = 10_000  # msgs/sec
INTERVAL = 1.0 / TARGET_RATE  # time between messages

logger.info(f"🚀 Starting real-time sensor data producer (~{TARGET_RATE:,} msg/sec)...")

# ------------------------------
# 🔁 Continuous Stream Loop
# ------------------------------
try:
    msg_count = 0
    last_flush = time.time()
    start_time = time.time()

    while True:
        mac_id = next(mac_cycle)
        msg = {
            "mac_id": mac_id,
            "temperature": round(random.uniform(20.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "timestamp_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }

        producer.send(TOPIC, msg)
        msg_count += 1

        # Flush every 2 seconds to maintain throughput
        if time.time() - last_flush >= 2:
            producer.flush()
            elapsed = time.time() - start_time
            rate = msg_count / elapsed
            logger.info(
                f"📡 Sent {msg_count:,} messages in {elapsed:.1f}s (~{rate:,.0f} msg/sec)"
            )
            last_flush = time.time()

        # Maintain ~10k msg/sec
        time.sleep(INTERVAL)

except KeyboardInterrupt:
    logger.warning("🛑 Producer interrupted manually. Exiting gracefully...")
except Exception as e:
    logger.exception(f"Unexpected error: {e}")
finally:
    if producer:
        producer.flush()
        producer.close()
    logger.info("✅ KafkaProducer closed and resources released.")
