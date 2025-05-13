"""
Avro Subscriber for e-commerce orders.
Consumes order messages from the Pub/Sub subscription in Avro binary format.
simulates one or multiple subscribers processing messages concurrently.
This script demonstrates how to consume Avro messages from a Pub/Sub subscription,
using the Google Cloud Pub/Sub client library and the fastavro library for Avro deserialization.
"""

import os
import io
import avro
from avro.io import BinaryDecoder, DatumReader
import threading
from concurrent.futures import ThreadPoolExecutor
import avro.schema
from google.cloud import pubsub_v1
from google.pubsub_v1.types import Schema

# Project configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
os.environ['GOOGLE_CLOUD_PROJECT'] = "elevated-column-458305-f8" # for local dev

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") # for local dev
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT environment variable must be set")

# Pub/Sub settings
SUBSCRIPTION_NAME = "orders-sub-avro"
SCHEMA_NAME = "orders-schema"

def get_schema():
    """Fetch the Avro schema from the schema registry."""
    schema_client = pubsub_v1.SchemaServiceClient()
    schema_path = schema_client.schema_path(PROJECT_ID, SCHEMA_NAME)

    # Get the schema definition
    schema = schema_client.get_schema(name=schema_path)
    # Check if the schema is of type AVRO and has a definition
    # If not, raise an error
    if not schema:
        raise ValueError(f"Schema {SCHEMA_NAME} not found.")
    if schema.type_ != Schema.Type.AVRO:
        raise ValueError(f"Schema {SCHEMA_NAME} is not of type AVRO.")
    if not schema.definition:
        raise ValueError(f"Schema {SCHEMA_NAME} has no definition.")

    print("Schema fetched from registry")
    return schema.definition

def deserialize_from_avro(binary_data, schema_str):
    """Deserialize Avro binary data using the provided schema."""
    try:
        schema = avro.schema.parse(schema_str)
        
        # Create a BytesIO object from the binary data
        bytes_io = io.BytesIO(binary_data)
        
        # Deserialize the binary data
        decoder = BinaryDecoder(bytes_io)
        reader = DatumReader(schema)

        order = reader.read(decoder)
        bytes_io.close()
        print(f"Deserialized Avro data")
        
        return order
    except Exception as e:
        print(f"Error deserializing Avro data: {e}")
        raise

def process_avro_message(message, schema_str):
    """Process a received Pub/Sub message in Avro format."""
    try:
        # Deserialize the Avro binary data
        order = deserialize_from_avro(message.data, schema_str)

        # Validate required keys
        required_keys = ['order_id', 'customer_id', 'status', 'total_amount', 'items', 'shipping_address']
        if not all(key in order for key in required_keys):
            raise KeyError(f"Missing required keys in order: {set(required_keys) - set(order.keys())}")
        
        # Print message details
        print(f"\nReceived Avro Order - ID: {order['order_id']}")
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
        print(f"Avro message acknowledged: {message.message_id}")
    
    except Exception as e:
        print(f"Error processing Avro message: {e}")
        # Negative acknowledgement in case of error
        message.nack()

def subscriber_process(subscriber_id, schema_str):
    """Run a subscriber process to consume Avro messages."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    print(f"Avro Subscriber {subscriber_id} started. Listening for messages...")
    
    # Counter for received messages
    received_messages = 0
    
    # Callback function to process incoming messages
    def callback(message):
        nonlocal received_messages
        print(f"\nAvro Subscriber {subscriber_id} received message:")
        process_avro_message(message, schema_str)
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
        print(f"Avro Subscriber {subscriber_id} error: {e}")
    finally:
        subscriber.close()
        print(f"Avro Subscriber {subscriber_id} finished. Processed {received_messages} messages.")

def main():
    """Main function to demonstrate multiple Avro subscribers."""
    # Fetch the schema from the registry
    schema_str = get_schema()
    print(f"Successfully fetched schema from registry")
    
    # Define the number of parallel subscribers
    num_subscribers = 2
    
    # Start multiple subscriber threads
    with ThreadPoolExecutor(max_workers=num_subscribers) as executor:
        for i in range(num_subscribers):
            executor.submit(
                subscriber_process, 
                i+1, # Subscriber ID
                schema_str # Pass the schema string to the subscriber process
            )

if __name__ == "__main__":
    main()