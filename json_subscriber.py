"""
JSON Subscriber for e-commerce orders.
Consumes order messages from the Pub/Sub subscription in JSON format.
This script simulates one or multiple subscribers processing messages concurrently.
"""

import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1

# Project configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
os.environ['GOOGLE_CLOUD_PROJECT'] = "elevated-column-458305-f8" # for local dev

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT environment variable must be set")

# Pub/Sub settings
SUBSCRIPTION_NAME = "orders-sub-json"
SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}"

def process_message(message):
    """Process a received Pub/Sub message."""
    try:
        # Extract message data
        message_data = message.data.decode("utf-8")
        order = json.loads(message_data)

        # Validate required keys
        required_keys = ['order_id', 'customer_id', 'status', 'total_amount', 'items', 'shipping_address']
        if not all(key in order for key in required_keys):
            raise KeyError(f"Missing required keys in order: {set(required_keys) - set(order.keys())}")
        
        # Print message details
        print(f"\nReceived Order - ID: {order['order_id']}")
        print(f"Customer: {order['customer_id']}")
        print(f"Status: {order['status']}")
        print(f"Total Amount: ${order['total_amount']}")
        print(f"Items: {len(order['items'])}")
        
        # Print order items
        for idx, item in enumerate(order['items'], start=1):
            print(f"  Item {idx}: {item['quantity']} x {item['product_name']} @ ${item['unit_price']} each")
        
        # Print shipping address
        addr = order['shipping_address']
        print(f"Shipping to: {addr['street']}, {addr['city']}, {addr['state']} {addr['zip']}, {addr['country']}")
        
        # Print message attributes
        if message.attributes:
            print("Message attributes:")
            for key, value in message.attributes.items():
                print(f"  {key}: {value}")
        
        # Acknowledge the message
        message.ack()
        print(f"Message acknowledged: {message.message_id}")
    
    except Exception as e:
        print(f"Error processing message: {e}")
        # Negative acknowledgement in case of error
        message.nack()

def subscriber_process(subscriber_id):
    """Run a subscriber process to consume messages."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    print(f"Subscriber {subscriber_id} started. Listening for messages...")
    
    # Counter for received messages
    received_messages = 0
    
    # Callback function to process incoming messages
    def callback(message):
        nonlocal received_messages
        print(f"\nSubscriber {subscriber_id} received message:")
        process_message(message)
        received_messages += 1
    
    # Create a streaming pull subscription
    streaming_pull_future = subscriber.subscribe(
        subscription=subscription_path,
        callback=callback
    )
    
    # Wait for the future to complete
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Subscriber {subscriber_id} error: {e}")
    finally:
        subscriber.close()
        print(f"Subscriber {subscriber_id} finished. Processed {received_messages} messages.")

def main():
    """Main function to demonstrate multiple subscribers."""
    # Define the number of parallel subscribers
    num_subscribers = 2
    
    # Start multiple subscriber threads
    with ThreadPoolExecutor(max_workers=num_subscribers) as executor:
        for i in range(num_subscribers):
            executor.submit(
                subscriber_process, 
                i+1 # Subscriber ID
            )

if __name__ == "__main__":
    main()