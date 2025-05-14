"""
JSON Publisher for e-commerce orders.
Publishes sample order data to Pub/Sub in JSON format.
This script simulates one or multiple publishers sending messages at regular intervals.
"""

import os
import json
import time
import threading
from google.cloud import pubsub_v1
from mock_data_generator import generate_random_order

# Project configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
os.environ['GOOGLE_CLOUD_PROJECT'] = "elevated-column-458305-f8" # for local dev

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")  # for local dev
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT environment variable must be set")

# Pub/Sub topic
TOPIC_NAME = "orders-topic"

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