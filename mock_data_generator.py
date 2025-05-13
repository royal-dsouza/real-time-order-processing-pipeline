import io
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import base64

# Define the schema
schema_str = """
{
    "type": "record",
    "name": "Order",
    "namespace": "com.ecommerce",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "order_date", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "total_amount", "type": "double"},
        {
            "name": "items",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "OrderItem",
                    "fields": [
                        {"name": "product_id", "type": "string"},
                        {"name": "product_name", "type": "string"},
                        {"name": "quantity", "type": "int"},
                        {"name": "unit_price", "type": "double"}
                    ]
                }
            }
        },
        {
            "name": "shipping_address",
            "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "state", "type": "string"},
                    {"name": "zip", "type": "string"},
                    {"name": "country", "type": "string"}
                ]
            }
        }
    ]
}
"""

# Parse the schema
schema = avro.schema.parse(schema_str)

# Example order data
order_data = {
    "order_id": "ORD-1234",
    "customer_id": "CUST-5678",
    "order_date": "2025-05-13T12:00:00Z",
    "status": "CREATED",
    "total_amount": 1234.56,
    "items": [
        {
            "product_id": "prod-001",
            "product_name": "Smartphone",
            "quantity": 1,
            "unit_price": 699.99
        }
    ],
    "shipping_address": {
        "street": "123 Main St",
        "city": "Los Angeles",
        "state": "CA",
        "zip": "90001",
        "country": "USA"
    }
}

# Serialize the data to Avro binary format
bytes_io = io.BytesIO()
encoder = BinaryEncoder(bytes_io)
writer = DatumWriter(schema)
writer.write(order_data, encoder)
avro_binary = bytes_io.getvalue()

# Base64 encode the Avro binary data
base64_encoded_data = base64.b64encode(avro_binary).decode("utf-8")

print(f"Base64 Encoded Avro Data: {base64_encoded_data}")