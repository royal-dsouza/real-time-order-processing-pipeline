import io
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import base64
import random
from datetime import datetime, timedelta
import uuid

schema_str_path = "orders.avsc"

# Read the Avro schema from a file
with open(schema_str_path, "r") as schema_file:
    schema_str = schema_file.read()

# Parse the schema
schema = avro.schema.parse(schema_str)

# Sample data for order generation (same as in json_publisher.py)
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
    """Generate a random order for testing (same logic as in json_publisher.py)."""
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

order_data = generate_random_order()

print(f"Generated Order: {order_data}")

# Serialize the data to Avro binary format
bytes_io = io.BytesIO()
encoder = BinaryEncoder(bytes_io)
writer = DatumWriter(schema)
writer.write(order_data, encoder)
avro_binary = bytes_io.getvalue()

# String representation of Base64 encoded Avro binary data this is to include in the message of JSON payload
base64_encoded_data = base64.b64encode(avro_binary).decode("utf-8")

print(f"Base64 Encoded Avro Data: {base64_encoded_data}")