
import os
import sys
import json
import time
from datetime import date

import pika

sys.path.insert(0, "/content/freshcart_elt")

import extract
import snowflake_io as sf

EXCHANGE = "rapido-dispatch"
QUEUE = "freshcart-lakehouse-bonus"

# Stable batch ID: reused for the second idempotency test.
BATCH_ID = os.getenv("BONUS_BATCH_ID", "bonus_stream_20260903_v1")
RUN_DATE = date(2026, 9, 3)

BASE = "/content/freshcart_elt"
SQL = f"{BASE}/sql"

# ---------- 1. CONNECT TO RABBITMQ ----------
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

channel.queue_declare(queue=QUEUE, durable=True)
channel.queue_bind(
    exchange=EXCHANGE,
    queue=QUEUE,
    routing_key="#"
)

channel.basic_qos(prefetch_count=20)

# ---------- 2. COLLECT FOR ONE MINUTE ----------
orders = []
delivery_tags = []

start = time.monotonic()
duration = 60

print("Collecting FreshCart orders for 60 seconds...")

try:
    while time.monotonic() - start < duration:
        method, properties, body = channel.basic_get(
            queue=QUEUE,
            auto_ack=False
        )

        if method is None:
            time.sleep(0.5)
            continue

        try:
            message = json.loads(body)
            order = message["order"]

            required = [
                "order_id", "cust_email", "cust_name",
                "order_date", "amount", "currency",
                "pincode", "updated_at"
            ]

            if not all(k in order for k in required):
                raise ValueError("Incomplete FreshCart order")

            orders.append(order)
            delivery_tags.append(method.delivery_tag)

            print("Received:", order["order_id"])

        except Exception as exc:
            print("Rejected invalid message:", exc)
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

    if not orders:
        raise RuntimeError("No valid orders received in 60 seconds")

    # ---------- 3. WRITE RAW LANDING FILE ----------
    landing_file = extract.write_landing(
        "web",
        orders,
        BATCH_ID,
        RUN_DATE
    )

    if landing_file is None:
        raise RuntimeError("Landing file was not created")

    # ACK only after the raw file is safely written.
    for tag in delivery_tags:
        channel.basic_ack(delivery_tag=tag)

    print("Landing file:", landing_file)
    print("Total orders collected:", len(orders))
    print("RabbitMQ messages acknowledged.")

except Exception:
    # Requeue unacknowledged messages if the landing write fails.
    for tag in delivery_tags:
        try:
            channel.basic_nack(
                delivery_tag=tag,
                requeue=True
            )
        except Exception:
            pass
    raise

finally:
    connection.close()


# ---------- 4. LOAD AND TRANSFORM IN SNOWFLAKE ----------
conn = sf.connect()

try:
    stage_dir = (
        f"stream/web/{RUN_DATE.isoformat()}/{BATCH_ID}"
    )

    loaded = sf.load_file(
        conn,
        landing_file,
        "bronze.orders_raw",
        stage_dir
    )

    print("New rows loaded into Bronze:", loaded)

    variables = {"run_id": BATCH_ID}

    sf.run_sql_file(
        conn,
        f"{SQL}/10_silver_orders_clean.sql",
        variables
    )
    print("Silver cleaning completed.")

    sf.run_sql_file(
        conn,
        f"{SQL}/20_silver_orders_dedup.sql",
        variables
    )
    print("Silver deduplication completed.")

    sf.run_sql_file(
        conn,
        f"{SQL}/40_gold_fct_orders.sql",
        variables
    )
    print("Gold transformation completed.")

    result = sf.query_one(
        conn,
        """
        SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points)
        FROM gold.fct_orders
        """
    )

    print("FINAL GOLD TOTALS")
    print("Orders:", result[0])
    print("Revenue INR:", result[1])
    print("Loyalty points:", result[2])

finally:
    conn.close()
