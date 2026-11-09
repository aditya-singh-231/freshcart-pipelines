
import pika
import random
import time
import uuid
import json
import argparse
from datetime import datetime, timezone

EXCHANGE = "rapido-dispatch"

cities = ["bangalore", "chennai"]

zones = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "anna-nagar", "velachery"]
}

types = ["ride", "food"]

parser = argparse.ArgumentParser()
parser.add_argument("--duration", type=int, default=120)
args = parser.parse_args()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

print("Dispatcher started...", flush=True)

start = time.monotonic()
count = 0

try:
    while time.monotonic() - start < args.duration:

        count += 1

        city = random.choice(cities)
        zone = random.choice(zones[city])
        order_type = random.choice(types)

        routing_key = f"{city}.{zone}.{order_type}"

        message = {
            "id": str(uuid.uuid4())[:8],
            "city": city,
            "zone": zone,
            "type": order_type,
            "ts": datetime.now(timezone.utc).isoformat()
        }

        bad_message = count % 10 == 0

        if bad_message:
            message.pop("zone")

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2
            )
        )

        if bad_message:
            print(
                f"Published BAD #{count}: {routing_key} | "
                "missing zone field",
                flush=True
            )
        else:
            print(
                f"Published GOOD #{count}: {routing_key}",
                flush=True
            )

        time.sleep(2)

finally:
    connection.close()
    print(f"Dispatcher stopped. Total messages: {count}")
