# Kafka to Snowflake Data Pipeline

This project demonstrates a complete data pipeline that generates mock e-commerce order data, streams it through Apache Kafka, and loads it into Snowflake with dimensional modeling.

## Overview

The system consists of several components working together:

1. **Zookeeper**: Coordinates the Kafka cluster
2. **Kafka**: Distributed streaming platform that handles the order messages
3. **Producer**: Generates mock order data and publishes to Kafka
4. **Consumer**: Reads from Kafka and loads data into Snowflake with proper dimensional modeling
5. **Snowflake**: Cloud data warehouse that stores the final processed data

## Architecture

```
Producer → Kafka → Consumer → Snowflake
```

## Components

### 1. Infrastructure (Docker Compose)

The `docker-compose.yml` file defines all services:

- **Zookeeper**: Single node for Kafka coordination
- **Kafka**: Single broker configuration
- **kafka-setup**: Service that waits for Kafka to be ready and creates the `orders` topic
- **producer**: Generates and sends mock order data to Kafka
- **consumer**: Consumes messages from Kafka and loads them into Snowflake

### **2. Producer Code (`producer.py`)**

#### **Key Components**
1. **Kafka Producer Setup**
   ```python
   conf = {'bootstrap.servers': 'kafka:9092'}
   producer = Producer(conf)
   ```
   - Connects to Kafka running in a Docker container (`kafka:9092`).
   - Uses `confluent_kafka` library for message production.

2. **Mock Data Generation**
   - **Products**: 5 predefined products with `id`, `name`, `category`, `price`, and `region`.
   - **Customers**: 5 predefined customers with `id`, `name`, and `region`.
   - **Orders**: Dynamically generated with:
     - Random product & customer selection.
     - Random quantity (1-5).
     - Timestamp within the last 30 days.

3. **Order Structure**
   ```python
   {
       "order_id": "O1234",
       "order_timestamp": "2023-10-01T12:34:56Z",
       "customer_id": "C987",
       "customer_name": "Ali Raza",
       "customer_region": "Punjab",
       "shipping_region": "Sindh",
       "product_id": "P123",
       "product_name": "Wireless Mouse",
       "category": "Accessories",
       "price": 2500,
       "quantity": 3
   }
   ```

4. **Message Delivery Callback**
   ```python
   def delivery_callback(err, msg):
       if err:
           print(f"ERROR: Message failed delivery: {err}")
       else:
           print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
   ```
   - Logs success/failure of message delivery to Kafka.

5. **Producing Messages**
   ```python
   for i in range(100):
       order = generate_order()
       producer.produce(topic, value=json.dumps(order), callback=delivery_callback)
       producer.flush()  # Ensures message is sent immediately
       time.sleep(random.uniform(0.5, 2))  # Simulate real-time streaming
   ```
   - Generates **100 orders**.
   - Each order is:
     1. Serialized to JSON.
     2. Published to Kafka.
     3. Flushed to ensure delivery.
     4. Delayed randomly (0.5–2s) to simulate real-world streaming.

### **3. Consumer Code (`consumer.py`)**

#### **Key Components**
1. **Snowflake Connection**
   ```python
   ctx = snowflake.connector.connect(
       user=os.getenv('SNOWFLAKE_USER'),
       password=os.getenv('SNOWFLAKE_PASSWORD'),
       account=os.getenv('SNOWFLAKE_ACCOUNT'),
       warehouse='COMPUTE_WH',
       database='E_COMMERCE',
       schema='SALES'
   )
   ```
   - Uses environment variables for credentials.
   - Connects to Snowflake with a warehouse (`COMPUTE_WH`), database (`E_COMMERCE`), and schema (`SALES`).

2. **Kafka Consumer Setup**
   ```python
   conf = {
       'bootstrap.servers': 'kafka:9092',
       'group.id': 'snowflake_loader',
       'auto.offset.reset': 'earliest'
   }
   consumer = Consumer(conf)
   consumer.subscribe(['orders'])
   ```
   - Subscribes to the `orders` topic.
   - Consumer group: `snowflake_loader` (for offset tracking).
   - Starts from the earliest message if no offset is committed.

3. **Dimensional Modeling Functions**
   - **`get_or_create_customer()`**:  
     - Checks if a customer exists in `DIM_CUSTOMER`.
     - If not, inserts a new record and returns the `CUSTOMER_KEY`.
   - **`get_or_create_product()`**:  
     - Similar logic for products in `DIM_PRODUCT`.
   - **`get_date_key()`**:  
     - Converts the order timestamp into a `DATE_KEY` (format: `YYYYMMDD`).
     - Populates `DIM_DATE` with additional date attributes (day, month, year, etc.).

4. **Message Processing Logic**
   ```python
   def process_message(msg):
       data = json.loads(msg.value())
       order_date = parse_timestamp(data['order_timestamp'])
       
       # Get or create dimension keys
       customer_key = get_or_create_customer(cursor, data['customer_id'], ...)
       product_key = get_or_create_product(cursor, data['product_id'], ...)
       date_key = get_date_key(cursor, order_date)
       
       # Insert into FACT_ORDERS
       cursor.execute("""
           INSERT INTO FACT_ORDERS (
               ORDER_ID, ORDER_TIMESTAMP, CUSTOMER_KEY,
               PRODUCT_KEY, DATE_KEY, QUANTITY, TOTAL_AMOUNT, SHIPPING_REGION
           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
       """, (data['order_id'], order_date, customer_key, product_key, date_key, 
             data['quantity'], data['price'] * data['quantity'], data['shipping_region']))
       
       ctx.commit()
   ```
   - Parses the Kafka message (JSON).
   - Ensures all dimension records exist (customers, products, dates).
   - Inserts a fact record into `FACT_ORDERS` with:
     - Foreign keys (`CUSTOMER_KEY`, `PRODUCT_KEY`, `DATE_KEY`).
     - Calculated `TOTAL_AMOUNT` (`price * quantity`).
     - Original `SHIPPING_REGION` from the product.

5. **Polling Loop**
   ```python
   while True:
       msg = consumer.poll(1.0)  # Wait up to 1s for messages
       if msg is None:
           continue
       if msg.error():
           continue  # Skip errors
       process_message(msg)
   ```
   - Continuously polls Kafka for new messages.
   - Processes each message sequentially.

6. **Error Handling**
   - **Snowflake Transactions**: Uses `commit()` on success or `rollback()` on failure.
   - **Logging**: Detailed logs for debugging (e.g., `"Inserting order O1234 into FACT_ORDERS"`).

### 4. Snowflake Schema

The consumer implements a star schema in Snowflake with:

- **Dimension Tables**:
  - `DIM_CUSTOMER`: Customer information
  - `DIM_PRODUCT`: Product information
  - `DIM_DATE`: Date attributes for analysis
- **Fact Table**:
  - `FACT_ORDERS`: Order transactions with foreign keys to dimensions

## How to Run

1. **Prerequisites**:
   - Docker and Docker Compose installed
   - Snowflake account with:
     - Warehouse: `COMPUTE_WH`
     - Database: `E_COMMERCE`
     - Schema: `SALES`
     - Appropriate tables created (see schema in other file)

2. **Start the system**:
   ```bash
   docker-compose up
   ```

3. **Observe the flow**:
   - Zookeeper and Kafka will start first
   - The setup service will create the `orders` topic
   - Producer will generate and send 100 orders
   - Consumer will process each order and load to Snowflake

## Key Features

1. **Containerized Architecture**: All components run in Docker containers
2. **Dependency Management**: Services wait for their dependencies using `wait-for-it.sh`
3. **Resilient Messaging**: Kafka ensures message delivery even if consumer is down
4. **Dimensional Modeling**: Proper star schema implementation in Snowflake
5. **Error Handling**: Comprehensive error handling and transaction management
6. **Mock Data Generation**: Realistic random data for demonstration

## Configuration

Key environment variables (set in docker-compose.yml):

- `BOOTSTRAP_SERVERS`: Kafka connection (kafka:9092)
- Snowflake credentials:
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_ACCOUNT`

## Monitoring

Check the logs of each service to monitor progress:

```bash
docker-compose logs -f [service_name]
```

Where `service_name` can be: producer, consumer, kafka, etc.

## Scaling

This setup uses a single instance of each service for simplicity, but could be scaled by:

1. Adding more Kafka brokers
2. Running multiple consumer instances in the same consumer group
3. Partitioning the Kafka topic

## Clean Up

To stop and remove all containers:

```bash
docker-compose down
```