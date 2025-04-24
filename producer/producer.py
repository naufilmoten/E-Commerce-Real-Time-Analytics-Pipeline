from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime, timedelta

# Configuration
#conf = {'bootstrap.servers': 'localhost:9092'}

# Use host.docker.internal instead of localhost
conf = {'bootstrap.servers': 'kafka:9092'}

producer = Producer(conf)
topic = 'orders'

# Mock data generators
products = [
    {"id": "P123", "name": "Wireless Mouse", "category": "Accessories", "price": 2500, "region": "Punjab"},
    {"id": "P124", "name": "Bluetooth Headphones", "category": "Accessories", "price": 4500, "region": "Sindh"},
    {"id": "P125", "name": "USB-C Cable", "category": "Accessories", "price": 800, "region": "KPK"},
    {"id": "P126", "name": "Laptop Stand", "category": "Accessories", "price": 1500, "region": "Balochistan"},
    {"id": "P127", "name": "Mechanical Keyboard", "category": "Accessories", "price": 6000, "region": "Punjab"}
]

customers = [
    {"id": "C987", "name": "Ali Raza", "region": "Punjab"},
    {"id": "C988", "name": "Fatima Khan", "region": "Sindh"},
    {"id": "C989", "name": "Ahmed Malik", "region": "KPK"},
    {"id": "C990", "name": "Sara Ahmed", "region": "Punjab"},
    {"id": "C991", "name": "Usman Shah", "region": "Balochistan"}
]

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        print(f"Message delivered to {topic} [{partition}] at offset {offset}")

def generate_order():
    product = random.choice(products)
    customer = random.choice(customers)
    return {
        "order_id": f"O{random.randint(1000, 9999)}",
        "order_timestamp": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat() + "Z",
        "customer_id": customer["id"],
        "customer_name": customer["name"],
        "customer_region": customer["region"],
        "shipping_region": product["region"], 
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 5)
    }

# Produce messages
for i in range(100):
    order = generate_order()
    producer.produce(topic, value=json.dumps(order), callback=delivery_callback)
    print(f"Produced order {order['order_id']}")
    producer.flush()  # Flush after each message to ensure delivery
    time.sleep(random.uniform(0.5, 2))

producer.flush()