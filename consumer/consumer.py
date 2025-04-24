from confluent_kafka import Consumer
import json
import snowflake.connector
from datetime import datetime
import os

try:
    print("Connecting to Snowflake...")
    # Snowflake connection
    ctx = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='E_COMMERCE',
        schema='SALES'
    )
    print("Connected to Snowflake")
    
    cursor = ctx.cursor()
    try:
        print("Testing Snowflake connection...")
        cursor.execute("SELECT current_version()")
        version = cursor.fetchone()[0]
        print(f"Snowflake version: {version}")
    finally:
        cursor.close()

    print("Setting up Kafka consumer...")
    # Kafka consumer setup
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'snowflake_loader',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['orders'])
    print("Kafka consumer ready and subscribed to 'orders' topic")

    def get_or_create_customer(cursor, customer_id, name, region):
        print(f"Checking or creating customer: {customer_id}")
        cursor.execute("SELECT CUSTOMER_KEY FROM DIM_CUSTOMER WHERE CUSTOMER_ID = %s", (customer_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO DIM_CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION) VALUES (%s, %s, %s)", (customer_id, name, region))
        cursor.execute("SELECT CUSTOMER_KEY FROM DIM_CUSTOMER WHERE CUSTOMER_ID = %s", (customer_id,))
        return cursor.fetchone()[0]

    def get_or_create_product(cursor, product_id, name, category, price):
        print(f"Checking or creating product: {product_id}")
        cursor.execute("SELECT PRODUCT_KEY FROM DIM_PRODUCT WHERE PRODUCT_ID = %s", (product_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO DIM_PRODUCT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE) VALUES (%s, %s, %s, %s)", (product_id, name, category, price))
        cursor.execute("SELECT PRODUCT_KEY FROM DIM_PRODUCT WHERE PRODUCT_ID = %s", (product_id,))
        return cursor.fetchone()[0]

    def get_date_key(cursor, order_date):
        print(f"Checking or creating date for: {order_date}")
        date_value = order_date.date()
        date_key = int(date_value.strftime('%Y%m%d'))
        cursor.execute("SELECT DATE_KEY FROM DIM_DATE WHERE DATE_KEY = %s", (date_key,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("""
            INSERT INTO DIM_DATE (DATE_KEY, DATE, DAY_OF_WEEK, DAY_NAME, MONTH, MONTH_NAME, QUARTER, YEAR)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            date_key,
            date_value,
            date_value.isoweekday(),
            date_value.strftime('%A'),
            date_value.month,
            date_value.strftime('%B'),
            (date_value.month - 1) // 3 + 1,
            date_value.year
        ))
        return date_key

    def process_message(msg):
        print(f"Processing message: {msg.value()[:100]}...")  # Log first 100 chars of message
        cursor = ctx.cursor()
        try:
            data = json.loads(msg.value())
            timestamp_str = data['order_timestamp']
            try:
                order_date = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                order_date = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')

            print(f"Creating or fetching customer key for: {data['customer_id']}")
            customer_key = get_or_create_customer(cursor, data['customer_id'], data['customer_name'], data['customer_region'])
            print(f"Creating or fetching product key for: {data['product_id']}")
            product_key = get_or_create_product(cursor, data['product_id'], data['product_name'], data['category'], data['price'])
            print(f"Creating or fetching date key for: {order_date}")
            date_key = get_date_key(cursor, order_date)

            # Insert into FACT_ORDERS including SHIPPING_REGION
            print(f"Inserting order {data['order_id']} into FACT_ORDERS")
            cursor.execute("""
                INSERT INTO FACT_ORDERS (
                    ORDER_ID, ORDER_TIMESTAMP, CUSTOMER_KEY,
                    PRODUCT_KEY, DATE_KEY, QUANTITY, TOTAL_AMOUNT, SHIPPING_REGION
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['order_id'], order_date, customer_key,
                product_key, date_key, data['quantity'], data['price'] * data['quantity'],
                data.get('shipping_region')
            ))

            ctx.commit()
            print(f"Order {data['order_id']} inserted successfully.")
        except Exception as e:
            ctx.rollback()
            print(f"Error processing order: {e}")
        finally:
            cursor.close()

    try:
        print("Starting consumer loop...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            process_message(msg)
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()
        ctx.close()
        print("Cleaned up consumer and Snowflake connection.")

except Exception as e:
    print(f"Fatal error: {e}")
