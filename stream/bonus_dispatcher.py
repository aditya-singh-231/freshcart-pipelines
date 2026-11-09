
import json
from datetime import datetime, timezone
import pika

EXCHANGE = "rapido-dispatch"
QUEUE = "freshcart-lakehouse-bonus"

orders = [
    {
        "order_id": "STREAM-2001",
        "cust_email": "priya.r@gmail.com",
        "cust_name": "Priya R",
        "order_date": "2026-09-03",
        "amount": "1500.00",
        "currency": "INR",
        "pincode": "600041",
        "updated_at": "2026-09-03 18:00:00"
    },
    {
        "order_id": "STREAM-2002",
        "cust_email": "arjun.k@yahoo.in",
        "cust_name": "Arjun K",
        "order_date": "2026-09-03",
        "amount": "2500.00",
        "currency": "INR",
        "pincode": "560034",
        "updated_at": "2026-09-03 18:05:00"
    },
    {
        "order_id": "STREAM-2003",
        "cust_email": "meera@outlook.com",
        "cust_name": "Meera S",
        "order_date": "2026-09-03",
        "amount": "3000.00",
        "currency": "INR",
        "pincode": "400050",
        "updated_at": "2026-09-03 18:10:00"
    }
]

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
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

channel.confirm_delivery()

for i, order in enumerate(orders, start=1):
    message = {
        "id": order["order_id"],
        "city": "bangalore",
        "zone": "koramangala",
        "type": "food",
        "ts": datetime.now(timezone.utc).isoformat(),
        "order": order
    }

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="bangalore.koramangala.food",
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=order["order_id"]
        ),
        mandatory=True
    )

    print(f"Published {i}: {order['order_id']}")

connection.close()
print("All 3 FreshCart orders published successfully.")
