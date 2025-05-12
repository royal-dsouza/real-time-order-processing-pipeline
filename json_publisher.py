"""
JSON Publisher for e-commerce orders.
Publishes sample order data to Pub/Sub in JSON format.
This script simulates one or multiple publishers sending messages at regular intervals.
"""

import os
import json
import time
import uuid
import random
from datetime import datetime
import threading
from google.cloud import pubsub_v1

# Project configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
os.environ['GOOGLE_CLOUD_PROJECT'] = "elevated-column-458305-f8" # for local dev

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")  # for local dev
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT environment variable must be set")

# Pub/Sub topic
TOPIC_NAME = "orders-topic"

# Sample data for order generation
PRODUCTS = [
    {"id": "prod-001", "name": "Smartphone", "price": 699.99},
    {"id": "prod-002", "name": "Laptop", "price": 1299.99},
    {"id": "prod-003", "name": "Headphones", "price": 149.99},
    {"id": "prod-004", "name": "Smart Watch", "price": 249.99},
    {"id": "prod-005", "name": "Tablet", "price": 499.99},
    {"id": "prod-006", "name": "Camera", "price": 599.99},
    {"id": "prod-007", "name": "Gaming Console", "price": 499.99},
    {"id": "prod-008", "name": "Bluetooth Speaker", "price": 89.99},
    {"id": "prod-009", "name": "Wireless Earbuds", "price": 129.99},
    {"id": "prod-010", "name": "External SSD", "price": 179.99}
]

STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
ORDER_STATUSES = ["CREATED", "PROCESSED", "SHIPPED", "DELIVERED", "CANCELLED"]

def generate_random_order():
    """Generate a random order for testing."""
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    customer_id = f"CUST-{uuid.uuid4().hex[:8]}"
    order_date = datetime.now().isoformat()
    status = "CREATED"  # New orders always start with CREATED status
    
    # Generate random items for the order
    num_items = random.randint(1, 5)
    items = []
    total_amount = 0.0
    
    for _ in range(num_items):
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        unit_price = product["price"]
        item_total = quantity * unit_price
        total_amount += item_total
        
        items.append({
            "product_id": product["id"],
            "product_name": product["name"],
            "quantity": quantity,
            "unit_price": unit_price
        })
    
    # Generate random shipping address
    shipping_address = {
        "street": f"{random.randint(100, 9999)} Main St",
        "city": f"City-{random.randint(1, 100)}",
        "state": random.choice(STATES),
        "zip": f"{random.randint(10000, 99999)}",
        "country": "USA"
    }
    
    # Create the complete order
    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date,
        "status": status,
        "total_amount": round(total_amount, 2),
        "items": items,
        "shipping_address": shipping_address
    }
    
    return order

def publish_message(publisher, topic_path, order_data):
    """Publish a message to the Pub/Sub topic."""
    # Convert the order to JSON string
    message_data = json.dumps(order_data).encode("utf-8")
    
    # Include the order_id as a message attribute
    future = publisher.publish(
        topic_path, 
        data=message_data,
        message_format="JSON",
        order_id=order_data["order_id"]
    )
    
    # Wait for the publish future to resolve
    message_id = future.result()
    
    return message_id

def publisher_process(publisher_id, num_messages, interval):
    """Simulate a publisher process sending messages at regular intervals."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    
    for i in range(num_messages):
        order = generate_random_order()
        try:
            message_id = publish_message(publisher, topic_path, order)
            print(f"Publisher {publisher_id} - Published message {i+1}/{num_messages} with ID: {message_id}")
            print(f"Order: {order['order_id']} - Total: ${order['total_amount']}")
        except Exception as e:
            print(f"Publisher {publisher_id} - Error publishing message: {e}")
        
        # Sleep for the specified interval
        time.sleep(interval)

def main():
    """Main function to demonstrate multiple publishers."""
    # Define the number of parallel publishers
    num_publishers = 3
    messages_per_publisher = 5
    interval_seconds = 2
    
    # Start multiple publisher threads
    threads = []
    for i in range(num_publishers):
        publisher_thread = threading.Thread(
            target=publisher_process,
            args=(i+1, messages_per_publisher, interval_seconds)
        )
        threads.append(publisher_thread)
        publisher_thread.start()
    
    # Wait for all publishers to complete
    for thread in threads:
        thread.join()
    
    print("All publishers completed successfully!")

if __name__ == "__main__":
    main()