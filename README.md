# Real-Time Order Processing Pipeline
A comprehensive, event-driven pipeline for real-time e-commerce order processing using Google Cloud services. This project demonstrates ingesting, transforming, and scheduling order data using Pub/Sub, Cloud Run, and Cloud Scheduler.

## Problem Statement
Build a real-time data pipeline that ingests customer orders published as JSON and Avro messages via Google Cloud Pub/Sub, processes them in a Cloud Run service, and demonstrates scheduled triggers via Cloud Scheduler.

Requirements:
1. Create a Pub/Sub topic `orders-topic` and two subscriptions: `orders-sub-json` and `orders-sub-avro`.
2. Implement a single publisher in Python to serialize and publish JSON orders to `orders-topic`.
3. Implement a subscriber in Python to consume and print JSON orders from `orders-sub-json`.
4. Extend to multiple publisher processes publishing to the same topic, and multiple subscriber processes consuming without duplication.
5. Define an Avro schema for the order payload, register it in Pub/Sub, and attach it to `orders-topic` with `BINARY` encoding.
6. Implement an Avro publisher that fetches the latest schema and publishes binary Avro messages.
7. Implement an Avro subscriber that deserializes messages according to the schema.
8. Deploy a Cloud Run service that triggers on Pub/Sub (`orders-topic`) and transforms orders (e.g., add fulfillment status) before logging.
9. Create a Cloud Scheduler job that sends an HTTP POST to the Cloud Run service every hour with a test payload.

## Solution

![real-time-order-processing drawio](https://github.com/user-attachments/assets/c2fc536c-520d-4be9-bcc2-35aeb3bafbac)


```bash
.
├── app.py                  # Cloud Run service handler
├── avro_publisher.py       # Avro publisher to Pub/Sub
├── avro_subscriber.py      # Avro subscriber from Pub/Sub
├── json_publisher.py       # JSON publisher to Pub/Sub
├── json_subscriber.py      # JSON subscriber from Pub/Sub
├── mock_data_generator.py  # Generates mock order data
├── orders.avsc             # Avro schema definition
├── test_payload.json       # Payload for Cloud Scheduler
├── Dockerfile              # Container config for Cloud Run
├── cloudbuild.yaml         # CI/CD pipeline
└── requirements.txt        # Python dependencies
```

1. **Created pusbsub topic `orders-topic` and json subscription `orders-sub-json`**
   ```bash
    # Create topic
    gcloud pubsub topics create orders-topic

    # Create json subscription
    gcloud pubsub subscriptions create orders-sub-json  --topic=orders-topic
   ```

2. **Created Mock data generator, JSON Publisher and Subscriber scripts**

    **mock_data_generator.py:** Generates sample order data in the specified schema
    
    **json_publisher.py:** Sends JSON messages simulating one or multiple publishers sending messages at regular intervals to `orders-topic` without duplication
    
    **json_subscriber.py:** Consumes the messages simulating one or multiple subscribers consuming messages and printing JSON messages from `orders-sub-json` without duplication

3. **Registered avro schema `orders-schema`, Updated topic `orders-topic` and created Avro subscription `orders-sub-avro`**
   ```bash
       # Register Avro schema
        gcloud pubsub schemas create orders-schema \
          --type=avro \
          --definition=orders.avsc
        
       # Create Avro subscription
        gcloud pubsub subscriptions create orders-sub-avro  --topic=orders-topic  
        
       # Update topic with Avro schema and BINARY encoding
        gcloud pubsub topics update orders-topic \
          --schema=orders-schema \
          --message-encoding=BINARY
   ```

4. **Created Avro Publisher and Subscriber scripts**
   
   **avro_publisher.py:** Sends serialised Avro messages simulating one or more publishers sending messages at regular intervals to `orders-topic`

   **avro_publisher.py:** Consumes the Avro messages simulating one or more subscribers consuming messages, deserializing it and printing the messages from `orders-topic` without duplication

5. **Created Cloud Run Service**

   **app.py:** Triggered by Pub/Sub, processes the order by adding fulfillment informaiton, updates status and logs processed orders into **firestore**

6. **Created Repository in Artifact Registry**
   
   ```bash
       # create repo in artifacts registry
      gcloud artifacts repositories create real-time-order-processing-pubsub-repo \
          --repository-format=docker \
          --location="us-central1"
   ```
   
8. **Setup CI/CD with Cloud Build**

   **dockerfile:** Defines build steps for Cloud Run.

   **cloudbuild.yaml:** Builds, pushes, and deploys the service.

   **Cloud Build trigger** is configured to deploy on repository changes.

9. **Created Push-Based Pub/Sub Trigger to Cloud Run**

   Created the subscription `push-orders-to-cloud-run` to push the data automatically to cloud run service http when messages are published to `orders-topic`
   ```bash
   gcloud pubsub subscriptions create push-orders-to-cloud-run \
    --topic=orders-topic \
    --push-endpoint=https://real-time-order-processing-pubsub-service-752749770357.us-central1.run.app \
    --ack-deadline=600
   ```

11. **Cloud Scheduler Job**

    **test_payload.json:** Sample order for scheduled POST for testing

    ```bash
    # scheduler job to send post request to cloud run service every hour
    gcloud scheduler jobs create http order-trigger \
      --location=us-central1 \
      --schedule="0 * * * *" \
      --uri="https://real-time-order-processing-pubsub-service-752749770357.us-central1.run.app" \
      --http-method=POST \
      --headers="Content-Type=application/json" \
      --message-body-from-file=test_payload.json
    ```

Acknowledgements

This project was developed as part of the assignment from GCP Data Engineering Mastery Bootcamp by Grow Data Skills, showcasing real-time data engineering skills on Google Cloud Platform.
