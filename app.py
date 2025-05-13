"""
Cloud Run service for processing orders from Pub/Sub and test HTTP endpoints.
"""

import os
from datetime import timedelta
import io
import base64
from flask import Flask, request, jsonify
import avro
from avro.io import BinaryDecoder, DatumReader
from datetime import datetime
from google.cloud import pubsub_v1
from google.pubsub_v1.types import Schema


app = Flask(__name__)

# Project configuration
# SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev
# os.environ['PROJECT_ID'] = "elevated-column-458305-f8" # for local dev

SCHEMA_NAME = os.getenv("SCHEMA_NAME", "orders-schema")

PROJECT_ID = os.getenv("PROJECT_ID", "elevated-column-458305-f8") 
if not PROJECT_ID:
    raise EnvironmentError("PROJECT_ID environment variable must be set")

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
        return None
    
def process_order(order):
    """Process an order by adding a fulfillment status and timestamp."""
    try:
        # Add fulfillment status and timestamp
        order['processing_timestamp'] = datetime.now().isoformat()
        order['status'] = "PROCESSED"

        # Add fulfillment information
        order['fulfillment'] = {
            "status": "FULFILLED",
            "timestamp": datetime.now().isoformat(),
            "estimated_delivery": (datetime.now() + timedelta(days=3)).isoformat(),
            "warehouse_id": f"WH-{order['shipping_address']['state']}"
        }

        print(f"Processed order {order['order_id']} with status {order['status']}")
        
        return order
    except Exception as e:
        print(f"Error processing order {order['order_id']}: {e}")
        return None
    
@app.route('/', methods=['POST'])
def process_pubsub_message():
    """Process incoming Pub/Sub messages."""
    try:
        # Get the Pub/Sub message from the request
        pubsub_message = request.get_json()
        
        # Decode the message data
        message_data = base64.b64decode(pubsub_message['message']['data'])
        
        # Fetch the Avro schema
        schema_str = get_schema()
        
        # Deserialize the Avro message
        order = deserialize_from_avro(message_data, schema_str)
        
        if order is None:
            return jsonify({"error": "Failed to deserialize message"}), 400
        
        # Process the order
        processed_order = process_order(order)
        
        if processed_order is None:
            return jsonify({"error": "Failed to process order"}), 500
        
        return jsonify(processed_order), 200
    
    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}")
        return jsonify({"error": str(e)}), 500
    
    except ValueError as ve:
        print(f"ValueError: {ve}")
        return jsonify({"error": str(ve)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)