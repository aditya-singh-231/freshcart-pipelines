
import pika
import json
import sys

EXCHANGE = "rapido-dispatch"

if len(sys.argv) != 3:
    print("Usage: python driver.py NAME PATTERN")
    sys.exit(1)

name = sys.argv[1]
pattern = sys.argv[2]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

result = channel.queue_declare(
    queue="",
    exclusive=True,
    auto_delete=True
)

queue_name = result.method.queue

channel.queue_bind(
    exchange=EXCHANGE,
    queue=queue_name,
    routing_key=pattern
)

channel.basic_qos(prefetch_count=1)

print(f"{name} started. Pattern: {pattern}", flush=True)

def receive_message(ch, method, properties, body):
    try:
        message = json.loads(body)

        print(
            f"{name} received: {method.routing_key} | {message}",
            flush=True
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"{name} processing error: {e}", flush=True)
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )

channel.basic_consume(
    queue=queue_name,
    on_message_callback=receive_message,
    auto_ack=False
)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print(f"{name} stopped.")
finally:
    if connection.is_open:
        connection.close()
