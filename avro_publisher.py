#!/usr/bin/env python3
"""
Avro Publisher for e-commerce orders.
Publishes sample order data to Pub/Sub in Avro binary format using the schema registry.
This script simulates one or multiple publishers sending messages concurrently.
"""

import os
import io
import time
import avro
from avro.io import BinaryEncoder, DatumWriter
import threading
from google.cloud import pubsub_v1
from google.pubsub_v1.types import Schema
from mock_data_generator import generate_random_order

# # Project configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
os.environ['GOOGLE_CLOUD_PROJECT'] = "elevated-column-458305-f8" # for local dev

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT","elevated-column-458305-f8")
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT environment variable must be set")

# Pub/Sub settings
TOPIC_NAME = "orders-topic"
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

def serialize_to_avro(order_data, schema_str):
    """Serialize the order data to Avro binary format."""
    try:
        schema = avro.schema.parse(schema_str)
        
        # Create a BytesIO object to hold the binary data
        bytes_io = io.BytesIO()

        encoder = BinaryEncoder(bytes_io)
        writer = DatumWriter(schema)

        # Serialize the order data into Avro binary format using the provided schema
        writer.write(order_data, encoder)
        
        avro_binary = bytes_io.getvalue()
        bytes_io.close()
        print(f"Serialized Avro data")
        
        return avro_binary
    except Exception as e:
        print(f"Error serializing to Avro: {e}")
        raise

def publish_avro_message(publisher, topic_path, order_data, schema_str):
    """Publish an Avro-encoded message to the Pub/Sub topic."""
    # Serialize the order to Avro binary format
    avro_binary = serialize_to_avro(order_data, schema_str)
    
    # publish the message to the topic
    future = publisher.publish(
        topic_path, 
        data=avro_binary
    )
    
    # Wait for the publish future to resolve
    message_id = future.result()
    
    return message_id

def publisher_process(publisher_id, schema_str, num_messages, interval):
    """Simulate a publisher process sending Avro messages at regular intervals."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    
    for i in range(num_messages):
        order = generate_random_order()
        try:
            message_id = publish_avro_message(publisher, topic_path, order, schema_str)
            print(f"Avro Publisher {publisher_id} - Published message {i+1}/{num_messages} with ID: {message_id}")
            print(f"Order: {order['order_id']} - Total: ${order['total_amount']}")
        except Exception as e:
            print(f"Avro Publisher {publisher_id} - Error publishing message: {e}")
        
        # Sleep for the specified interval
        time.sleep(interval)

def main():
    """Main function to demonstrate multiple Avro publishers."""
    # Fetch the schema from the registry
    schema_str = get_schema()
    print(f"Successfully fetched schema from registry")
    
    # Define the number of parallel publishers
    num_publishers = 2
    messages_per_publisher = 3
    interval_seconds = 3
    
    # Start multiple publisher threads
    threads = []
    for i in range(num_publishers):
        publisher_thread = threading.Thread(
            target=publisher_process,
            args=(i+1, schema_str, messages_per_publisher, interval_seconds)
        )
        threads.append(publisher_thread)
        publisher_thread.start()
    
    # Wait for all publishers to complete
    for thread in threads:
        thread.join()
    
    print("All Avro publishers completed successfully!")

if __name__ == "__main__":
    main()