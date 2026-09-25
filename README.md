# Event-Driven Order Processing System

An event-driven Order Processing System implemented using **Java 17, Spring Boot, and Apache Kafka**.

The system demonstrates asynchronous communication between independent services using Kafka events, including producers, consumers, topics, partitions, consumer groups, message keys, manual offset management, JSON serialization/deserialization, retry handling, Dead Letter Topic (DLT), failure recovery, and multiple consumers.

---

## 1. Project Overview

### Technology Stack

* Java 17
* Spring Boot
* Spring Kafka
* Apache Kafka
* Jackson JSON
* Lombok
* REST API
* Maven
* Kafka UI
* Postman

### Microservices

| Service              | Responsibility                                       |
| -------------------- | ---------------------------------------------------- |
| Order Service        | Creates orders and publishes `OrderCreatedEvent`     |
| Payment Service      | Consumes orders and processes payment                |
| Notification Service | Consumes payment results and generates notifications |

---

# 2. Event Flow

```text
                    REST API
                       |
                       v
              +------------------+
              |   Order Service  |
              +------------------+
                       |
                       | OrderCreatedEvent
                       v
              +------------------+
              | Kafka            |
              | order-created    |
              | 3 Partitions     |
              +------------------+
                       |
                       v
              +------------------+
              | Payment Service  |
              +------------------+
                 /           \
                /             \
               v               v
      payment-completed   payment-failed
             |                 |
             +--------+--------+
                      |
                      v
              +------------------+
              | Kafka            |
              +------------------+
                      |
                      v
              +----------------------+
              | Notification Service |
              +----------------------+
                       |
                       v
              notification-events
                       |
                       v
                 Audit Consumer
```

### Complete asynchronous flow

```text
POST /api/v1/orders
        |
        v
Order Service
        |
        | Kafka: order-created
        v
Payment Service
        |
        +---- SUCCESS ----> payment-completed
        |
        +---- FAILURE ----> payment-failed
                              |
                              v
                    Notification Service
                              |
                              | notification-events
                              v
                        Audit Consumer
```

---

# 3. Project Structure

```text
order-processing-system/
│
├── order-service/
│   └── src/main/java/
│       └── com/example/orderservice/
│           ├── config/
│           ├── consumer/
│           ├── controller/
│           ├── dto/
│           ├── event/
│           ├── exception/
│           ├── model/
│           └── service/
│
├── payment-service/
│   └── src/main/java/
│       └── com/example/paymentservice/
│           ├── config/
│           ├── event/
│           ├── exception/
│           ├── model/
│           └── payment/
│
└── notification-service/
    └── src/main/java/
        └── com/example/notificationservice/
            ├── config/
            ├── event/
            └── notification/
```

---

# 4. Kafka Topics

The application uses the following Kafka topics:

| Topic                  | Partitions | Purpose                                      |
| ---------------------- | ---------: | -------------------------------------------- |
| `order-created`        |          3 | Order Service → Payment Service              |
| `payment-completed`    |          3 | Payment Service → Notification/Order Service |
| `payment-failed`       |          3 | Payment Service → Notification/Order Service |
| `notification-events`  |          3 | Notification events                          |
| `order-processing-dlt` |          3 | Failed messages after retry exhaustion       |

All application topics are configured with **3 partitions**.

> Note: The configured replication factor is `1`, which is suitable for a local single-broker demonstration environment but does not provide broker-level redundancy.

---

# 5. Kafka Topic Configuration

Topic creation is handled using Spring Kafka `NewTopic` and `TopicBuilder`.

### File

```text
order-service/src/main/java/com/example/orderservice/config/KafkaTopicConfig.java
```

```java
@Bean
public NewTopic orderCreatedTopic() {
    return TopicBuilder.name(orderCreatedTopic)
            .partitions(3)
            .replicas(1)
            .build();
}

@Bean
public NewTopic paymentCompletedTopic() {
    return TopicBuilder.name(paymentCompletedTopic)
            .partitions(3)
            .replicas(1)
            .build();
}

@Bean
public NewTopic paymentFailedTopic() {
    return TopicBuilder.name(paymentFailedTopic)
            .partitions(3)
            .replicas(1)
            .build();
}

@Bean
public NewTopic deadLetterTopic() {
    return TopicBuilder.name(dltTopic)
            .partitions(3)
            .replicas(1)
            .build();
}
```

### Notification Service Topic Configuration

**File:**

```text
notification-service/src/main/java/com/example/notificationservice/config/KafkaTopicConfig.java
```

```java
@Bean
public NewTopic notificationEventsTopic() {
    return TopicBuilder.name(notificationEventsTopic)
            .partitions(3)
            .replicas(1)
            .build();
}
```

The Notification Service also creates the `payment-completed`, `payment-failed`, and DLT topics if they do not already exist.

---

# 6. Partition Strategy

All major Kafka topics use **3 partitions**.

The producer sends messages using the **Order ID as the Kafka message key**.

Example:

```java
kafkaTemplate.send(orderCreatedTopic, orderId, event);
```

The same strategy is used for payment and notification events:

```java
kafkaTemplate.send(paymentCompletedTopic, event.getOrderId(), completedEvent);
```

and:

```java
kafkaTemplate.send(paymentFailedTopic, event.getOrderId(), failedEvent);
```

### Why Order ID is used as the key

Using `orderId` as the key allows Kafka's partitioning mechanism to consistently route messages for the same order to the same partition.

This helps preserve ordering for events belonging to the same order while allowing different orders to be distributed across the available partitions.

```text
Order A ──> Partition 0
Order B ──> Partition 1
Order C ──> Partition 2
Order A ──> Partition 0
```

The exact partition is determined by Kafka's partitioner/hash calculation.

---

# 7. Consumer Groups

The implementation uses separate consumer groups for different responsibilities.

### Main Consumer Groups

| Consumer Group         | Service              | Purpose                        |
| ---------------------- | -------------------- | ------------------------------ |
| `payment`              | Payment Service      | Consumes `order-created`       |
| `order-status-updater` | Order Service        | Consumes payment results       |
| `notification`         | Notification Service | Consumes payment results       |
| `audit`                | Notification Service | Consumes `notification-events` |
| `dlt-monitor`          | Notification Service | Monitors DLT messages          |

Consumer group IDs are configured through application properties.

---

# 8. Consumer Group Behavior

A Kafka consumer group allows multiple consumers to share the processing of partitions.

For example, the Payment Service is configured with:

```java
factory.setConcurrency(3);
```

and:

```java
@KafkaListener(
        topics = "${app.kafka.topics.order-created}",
        groupId = "${app.kafka.consumer-groups.payment}",
        containerFactory = "kafkaListenerContainerFactory")
```

With:

```text
order-created
 ├── Partition 0
 ├── Partition 1
 └── Partition 2

Payment Consumer Group
 ├── Consumer 1
 ├── Consumer 2
 └── Consumer 3
```

The three consumers can process the three partitions concurrently.

If there are fewer consumers than partitions, a consumer can receive multiple partitions.

If there are more consumers than partitions, some consumers remain idle because a partition can be assigned to only one consumer within the same consumer group.

---

# 9. Multiple Consumers

The implementation demonstrates multiple consumers through Spring Kafka concurrency.

### Configuration

**File:**

```text
payment-service/src/main/java/com/example/paymentservice/config/KafkaConsumerConfig.java
```

```java
factory.setConcurrency(3);
```

The same concurrency configuration is used in the Order Service and Notification Service where required.

Notification Service also contains separate consumers for:

```text
payment-completed
payment-failed
notification-events
DLT
```

This demonstrates multiple consumer responsibilities and consumer-group behavior.

---

# 10. Producer Configuration

Kafka producers are configured using Spring Kafka `ProducerFactory` and `KafkaTemplate`.

### File

```text
order-service/src/main/java/com/example/orderservice/config/KafkaProducerConfig.java
```

### Important Configuration

```java
configProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers
);

configProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class
);

configProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        JsonSerializer.class
);

configProps.put(
        ProducerConfig.ACKS_CONFIG,
        acks
);

configProps.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
        true
);

configProps.put(
        ProducerConfig.RETRIES_CONFIG,
        retries
);

configProps.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        5
);

configProps.put(
        JsonSerializer.ADD_TYPE_INFO_HEADERS,
        true
);
```

### Producer Settings

| Configuration            | Value / Purpose      |
| ------------------------ | -------------------- |
| Bootstrap Servers        | Kafka broker address |
| Key Serializer           | `StringSerializer`   |
| Value Serializer         | `JsonSerializer`     |
| Acknowledgement          | `all` by default     |
| Idempotence              | Enabled              |
| Producer Retries         | 5 by default         |
| Max In-Flight Requests   | 5                    |
| Type Information Headers | Enabled              |

The producer uses `KafkaTemplate` to publish events asynchronously.

Example:

```java
kafkaTemplate.send(orderCreatedTopic, orderId, event);
```

---

# 11. JSON Serialization

Kafka message values are exchanged as JSON.

Producer:

```java
JsonSerializer
```

Consumer:

```java
JsonDeserializer
```

The producer also adds type information headers:

```java
configProps.put(
        JsonSerializer.ADD_TYPE_INFO_HEADERS,
        true
);
```

The consumer uses trusted packages and type mappings to deserialize the incoming JSON event into the appropriate Java event class.

---

# 12. Consumer Configuration

### File

```text
payment-service/src/main/java/com/example/paymentservice/config/KafkaConsumerConfig.java
```

### Important Configuration

```java
props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers
);

props.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        false
);

props.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"
);

props.put(
        ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG,
        3000
);

props.put(
        ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
        10000
);

props.put(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        50
);
```

### Consumer Settings

| Configuration                | Purpose                                                              |
| ---------------------------- | -------------------------------------------------------------------- |
| `enable.auto.commit=false`   | Disables automatic offset commits                                    |
| `auto.offset.reset=earliest` | Reads from earliest available offset when no committed offset exists |
| `max.poll.records=50`        | Maximum records returned per poll                                    |
| Reconnect backoff            | Helps recover from temporary broker connectivity issues              |

---

# 13. Manual Offset Management

Automatic offset commits are disabled:

```java
props.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        false
);
```

The listener container uses:

```java
factory.getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
```

The consumer explicitly acknowledges successful processing:

```java
acknowledgment.acknowledge();
```

### Processing flow

```text
Kafka message
     |
     v
Consumer receives message
     |
     v
Business processing
     |
     +---- Failure ----> Retry / DLT
     |
     +---- Success ----> acknowledge()
                              |
                              v
                       Offset committed
```

This prevents the application from committing the offset before successful processing.

---

# 14. Why Manual Offset Commit Is Used

Manual acknowledgement gives the application control over when a Kafka record is considered successfully processed.

For example:

```java
PaymentStatus paymentStatus =
        paymentService.processPayment(event);

acknowledgment.acknowledge();
```

The acknowledgement happens after payment processing.

If processing throws an exception before acknowledgement, the record is handled by the configured Kafka error handler.

---

# 15. Error Handling and Retry

The implementation uses Spring Kafka:

```text
DefaultErrorHandler
+
ExponentialBackOff
+
DeadLetterPublishingRecoverer
```

### File

```text
payment-service/src/main/java/com/example/paymentservice/config/KafkaConsumerConfig.java
```

### Configuration

```java
ExponentialBackOff backOff =
        new ExponentialBackOff(
                retryInitialIntervalMs,
                retryMultiplier
        );

backOff.setMaxElapsedTime(
        retryMaxElapsedTimeMs
);

DefaultErrorHandler handler =
        new DefaultErrorHandler(
                recoverer,
                backOff
        );
```

Default values:

```text
Initial interval = 1000 ms
Multiplier        = 2.0
Maximum elapsed   = 15000 ms
```

The recoverer sends messages that cannot be successfully processed after retry attempts to:

```text
order-processing-dlt
```

---

# 16. Dead Letter Topic

The Dead Letter Topic is:

```text
order-processing-dlt
```

After retry attempts are exhausted, the failed Kafka record is routed to the DLT using:

```java
DeadLetterPublishingRecoverer
```

The DLT monitor consumes the failed message and logs information such as:

```text
Original topic
Message key
Partition
Offset
Exception
Message value
```

### DLT Monitor

**File:**

```text
notification-service/src/main/java/com/example/notificationservice/notification/DltMonitorConsumer.java
```

The DLT monitor reads headers such as:

```java
"kafka_dlt-original-topic"
```

and:

```java
"kafka_dlt-exception-message"
```

---

# 17. Failure Handling Strategy

The implementation demonstrates different failure scenarios.

## Scenario 1 — Successful Payment

```text
Order amount <= 5000
        |
        v
Payment processing succeeds
        |
        v
PaymentCompletedEvent
        |
        v
Notification Service
```

Example:

```json
{
  "customerId": "CUST-1001",
  "amount": 99.99
}
```

Result:

```text
PAYMENT_SUCCESSFUL
```

---

## Scenario 2 — Business Failure

For amounts greater than `5000.00`, the payment is rejected.

```text
Amount > 5000
      |
      v
Payment validation
      |
      v
PaymentFailedEvent
      |
      v
Notification Service
```

Failure reason:

```text
Transaction limit exceeded: max allowable amount is $5000.00
```

This is treated as a business failure rather than a retryable infrastructure failure.

---

## Scenario 3 — Retry and DLT

An amount of exactly:

```text
9999.00
```

is used to simulate a temporary payment gateway timeout.

The payment service throws:

```java
TransientPaymentException
```

The Kafka error handler performs exponential retry.

```text
OrderCreatedEvent
       |
       v
Payment Service
       |
       X
TransientPaymentException
       |
       v
Retry
       |
       v
Retry
       |
       v
Retry exhausted
       |
       v
order-processing-dlt
```

The DLT monitor logs the failure.

---

# 18. Failure and Recovery Test Results

The following scenarios can be demonstrated during the final review.

| Test                     | Input      | Expected Result         |
| ------------------------ | ---------- | ----------------------- |
| Successful payment       | `99.99`    | `PAYMENT_SUCCESSFUL`    |
| Business payment failure | `6000.00`  | `PAYMENT_FAILED`        |
| Retry scenario           | `9999.00`  | Exponential retry → DLT |
| Missing customer ID      | Missing    | HTTP `400`              |
| Missing amount           | Missing    | HTTP `400`              |
| Zero amount              | `0`        | HTTP `400`              |
| Unknown order ID         | Invalid ID | HTTP `404`              |

### Recovery Behavior

When a consumer fails while processing a retryable event:

```text
Consumer processing
       |
       v
Exception
       |
       v
DefaultErrorHandler
       |
       v
Exponential Backoff
       |
       +---- Successful retry ---> Processing continues
       |
       +---- Retry exhausted ---> DLT
```

When the consumer/container reconnects to Kafka, Kafka's consumer-group mechanism allows partition assignment and processing to continue from the appropriate committed offset.

---

# 19. API Details

Base URL examples:

```text
Order Service:        http://localhost:<order-service-port>
Payment Service:      http://localhost:8090
Notification Service: http://localhost:8087
```

Use the actual configured Order Service port from the application's configuration.

---

## 19.1 Create Order — Successful Payment

### Request

```http
POST {{orderService}}/api/v1/orders
Content-Type: application/json
```

```json
{
  "customerId": "CUST-1001",
  "amount": 99.99
}
```

### Response

```http
HTTP/1.1 202 ACCEPTED
```

```json
{
  "orderId": "a1b2c3d4-e5f6-4a1b-8c9d-1234567890ab",
  "customerId": "CUST-1001",
  "amount": 99.99,
  "status": "PENDING",
  "message": "Order submitted successfully and is processing asynchronously."
}
```

The response is initially `PENDING` because the processing is asynchronous.

After Kafka processing, call:

```http
GET {{orderService}}/api/v1/orders/{{orderId}}
```

Expected status:

```text
PAYMENT_SUCCESSFUL
```

---

# 20. Create Order — Payment Failed

### Request

```http
POST {{orderService}}/api/v1/orders
Content-Type: application/json
```

```json
{
  "customerId": "CUST-2002",
  "amount": 6000.00
}
```

### Initial Response

```http
HTTP/1.1 202 ACCEPTED
```

```json
{
  "orderId": "f9e8d7c6-b5a4-4c3d-9e8f-abcdef123456",
  "customerId": "CUST-2002",
  "amount": 6000.00,
  "status": "PENDING",
  "message": "Order submitted successfully and is processing asynchronously."
}
```

After asynchronous processing:

```http
GET {{orderService}}/api/v1/orders/{{orderId}}
```

Example response:

```json
{
  "orderId": "f9e8d7c6-b5a4-4c3d-9e8f-abcdef123456",
  "customerId": "CUST-2002",
  "amount": 6000.00,
  "status": "PAYMENT_FAILED",
  "createdAt": "2026-09-24T10:15:30.123"
}
```

Failure reason:

```text
Transaction limit exceeded: max allowable amount is $5000.00
```

---

# 21. Create Order — Retry and DLT

### Request

```http
POST {{orderService}}/api/v1/orders
Content-Type: application/json
```

```json
{
  "customerId": "CUST-3003",
  "amount": 9999.00
}
```

### Response

```http
HTTP/1.1 202 ACCEPTED
```

```json
{
  "orderId": "<generated-order-id>",
  "customerId": "CUST-3003",
  "amount": 9999.00,
  "status": "PENDING",
  "message": "Order submitted successfully and is processing asynchronously."
}
```

### Expected behavior

```text
TransientPaymentException
        ↓
Retry
        ↓
Exponential Backoff
        ↓
Retry Exhausted
        ↓
order-processing-dlt
```

The DLT monitor should log the failed record.

The order remains `PENDING` because the implementation does not publish either `PaymentCompletedEvent` or `PaymentFailedEvent` for this simulated transient exception.

---

# 22. Validation Error — Missing Customer ID

### Request

```http
POST {{orderService}}/api/v1/orders
Content-Type: application/json
```

```json
{
  "amount": 100.00
}
```

### Response

```http
HTTP/1.1 400 BAD REQUEST
```

```json
{
  "customerId": "customerId is mandatory"
}
```

---

# 23. Validation Error — Invalid Amount

### Request

```json
{
  "customerId": "CUST-1",
  "amount": 0
}
```

### Response

```http
HTTP/1.1 400 BAD REQUEST
```

```json
{
  "amount": "amount must be greater than zero"
}
```

---

# 24. Validation Error — Missing Amount

### Request

```json
{
  "customerId": "CUST-1"
}
```

### Response

```http
HTTP/1.1 400 BAD REQUEST
```

```json
{
  "amount": "amount is mandatory"
}
```

---

# 25. Get All Orders

### Request

```http
GET {{orderService}}/api/v1/orders
```

### Response

```http
HTTP/1.1 200 OK
```

```json
[
  {
    "orderId": "f9e8d7c6-b5a4-4c3d-9e8f-abcdef123456",
    "customerId": "CUST-2002",
    "amount": 6000.00,
    "status": "PAYMENT_FAILED",
    "createdAt": "2026-09-24T10:15:30.123"
  },
  {
    "orderId": "a1b2c3d4-e5f6-4a1b-8c9d-1234567890ab",
    "customerId": "CUST-1001",
    "amount": 99.99,
    "status": "PAYMENT_SUCCESSFUL",
    "createdAt": "2026-09-24T10:14:10.456"
  }
]
```

Orders are returned newest first.

---

# 26. Get Order By ID

### Request

```http
GET {{orderService}}/api/v1/orders/{{orderId}}
```

### Response — 200 OK

```json
{
  "orderId": "a1b2c3d4-e5f6-4a1b-8c9d-1234567890ab",
  "customerId": "CUST-1001",
  "amount": 99.99,
  "status": "PAYMENT_SUCCESSFUL",
  "createdAt": "2026-09-24T10:14:10.456"
}
```

### Response — 404 Not Found

If the order ID does not exist:

```http
HTTP/1.1 404 NOT FOUND
```

---

# 27. Health Checks

### Order Service

```http
GET {{orderService}}/actuator/health
```

Example:

```json
{
  "status": "UP",
  "components": {
    "ping": {
      "status": "UP"
    }
  }
}
```

### Payment Service

```http
GET {{paymentService}}:8090/actuator/health
```

### Notification Service

```http
GET {{notificationService}}:8087/actuator/health
```

---

# 28. Event Definitions

## OrderCreatedEvent

```text
eventId
orderId
customerId
amount
timestamp
```

Published to:

```text
order-created
```

---

## PaymentCompletedEvent

```text
paymentId
orderId
customerId
amount
timestamp
```

Published to:

```text
payment-completed
```

---

## PaymentFailedEvent

```text
paymentId
orderId
customerId
amount
failureReason
timestamp
```

Published to:

```text
payment-failed
```

---

## NotificationEvent

```text
notificationId
orderId
customerId
recipient
message
status
sentAt
```

Published to:

```text
notification-events
```

---

# 29. End-to-End Implementation

## Successful Flow

```text
1. Client sends POST /api/v1/orders
                  |
                  v
2. Order Service creates order
                  |
                  v
3. Order status = PENDING
                  |
                  v
4. OrderCreatedEvent published
                  |
                  v
5. Kafka order-created
                  |
                  v
6. Payment Service consumes event
                  |
                  v
7. Payment succeeds
                  |
                  v
8. PaymentCompletedEvent published
                  |
                  v
9. Kafka payment-completed
                  |
                  v
10. Notification Service consumes event
                  |
                  v
11. NotificationEvent generated
                  |
                  v
12. Kafka notification-events
                  |
                  v
13. Audit Consumer processes notification
                  |
                  v
14. Order Service receives payment result
                  |
                  v
15. Order status = PAYMENT_SUCCESSFUL
```

## Failed Payment Flow

```text
POST Order
   |
   v
order-created
   |
   v
Payment Service
   |
   | amount > 5000
   v
PaymentFailedEvent
   |
   v
payment-failed
   |
   v
Notification Service
   |
   v
Failure Notification
   |
   v
Order Service
   |
   v
PAYMENT_FAILED
```

---

# 30. Asynchronous Processing

The Create Order API returns:

```text
202 ACCEPTED
```

instead of waiting for the entire payment and notification flow.

The order is initially stored as:

```text
PENDING
```

Kafka then handles the asynchronous communication between services.

```text
REST Request
     |
     v
Order Service
     |
     | returns 202
     v
Client

Meanwhile:

Order Service
     |
     v
Kafka
     |
     v
Payment Service
     |
     v
Kafka
     |
     v
Notification Service
```

This demonstrates asynchronous event-driven communication.

---

# 31. Consumer Failure and Recovery

The consumer configuration disables automatic commits:

```java
ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG = false
```

and uses:

```java
AckMode.MANUAL_IMMEDIATE
```

Therefore, the application acknowledges the record after successful processing.

If processing fails:

```text
Message
  |
  v
Consumer
  |
  X
Exception
  |
  v
DefaultErrorHandler
  |
  v
Retry
  |
  +---- Success ---> acknowledge offset
  |
  +---- Failure ---> DLT
```

This provides controlled processing and failure handling.

---

# 32. Offset Management

### Configuration

```java
props.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        false
);
```

### Listener Container

```java
factory.getContainerProperties()
        .setAckMode(
            ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );
```

### Successful Processing

```java
ack.acknowledge();
```

The implementation logs partition and offset information for troubleshooting and demonstration.

Example log information:

```text
Partition: 1
Offset: 25
Key: <orderId>
```

---

# 33. Message Key Strategy

The system uses:

```text
orderId
```

as the Kafka message key.

Example:

```java
kafkaTemplate.send(
        orderCreatedTopic,
        orderId,
        event
);
```

Payment events also use:

```java
event.getOrderId()
```

as the key.

This provides consistent partitioning for events belonging to the same order.

---

# 34. Logging

The implementation uses Lombok `@Slf4j`.

Important information logged includes:

* Order ID
* Event key
* Topic
* Partition
* Offset
* Consumer thread
* Payment result
* Retry information
* DLT information
* Exceptions
* Offset acknowledgement

Example:

```text
PaymentConsumer received record
Key: [order-id]
Partition: [1]
Offset: [15]
```

Example successful publication:

```text
PaymentCompletedEvent published
Topic: [payment-completed]
Partition: [2]
Offset: [10]
Key: [order-id]
```

---

# 35. Kafka Architecture Demonstration

The final demonstration should show:

### Kafka Topics

```text
order-created
payment-completed
payment-failed
notification-events
order-processing-dlt
```

### Partitions

Each topic:

```text
Partition 0
Partition 1
Partition 2
```

### Consumer Groups

```text
payment
order-status-updater
notification
audit
dlt-monitor
```

---

# 36. Suggested Kafka UI Demonstration

Using Kafka UI:

```text
http://localhost:8080
```

Demonstrate:

1. List Kafka topics.
2. Open `order-created`.
3. Show 3 partitions.
4. Create an order using Postman.
5. Show the `OrderCreatedEvent`.
6. Show the message key as `orderId`.
7. Show Payment Service consuming the event.
8. Open `payment-completed` or `payment-failed`.
9. Show the corresponding payment event.
10. Open `notification-events`.
11. Show the generated notification event.
12. Trigger the `9999.00` retry scenario.
13. Open `order-processing-dlt`.
14. Show the failed message and DLT headers.

---

# 37. Screenshots / Evidence

The following screenshots should be added to the GitHub repository or README.

### Recommended Screenshots

```text
docs/
├── 01-project-running.png
├── 02-postman-create-order.png
├── 03-order-created-topic.png
├── 04-payment-service-consumer.png
├── 05-payment-completed-topic.png
├── 06-payment-failed-topic.png
├── 07-notification-events.png
├── 08-partitions.png
├── 09-consumer-groups.png
├── 10-manual-offset-logs.png
├── 11-retry-logs.png
├── 12-dlt-message.png
└── 13-end-to-end-flow.png
```

These screenshots provide evidence for the implementation and final demonstration.

---

# 38. End-to-End Test Cases

| Test Case | Input                    | Expected Result                                  |
| --------- | ------------------------ | ------------------------------------------------ |
| TC-01     | Amount `99.99`           | Payment succeeds                                 |
| TC-02     | Amount `6000`            | Payment fails                                    |
| TC-03     | Amount `9999`            | Retry → DLT                                      |
| TC-04     | Missing customer ID      | `400 Bad Request`                                |
| TC-05     | Missing amount           | `400 Bad Request`                                |
| TC-06     | Amount `0`               | `400 Bad Request`                                |
| TC-07     | Valid Order ID           | `200 OK`                                         |
| TC-08     | Invalid Order ID         | `404 Not Found`                                  |
| TC-09     | Multiple orders          | Messages distributed across partitions           |
| TC-10     | Consumer restart/failure | Consumer group rebalances and resumes processing |

---

# 39. Configuration Summary

| Area                       | Configuration          |
| -------------------------- | ---------------------- |
| Kafka Serialization        | JSON                   |
| Key Serialization          | String                 |
| Value Serialization        | JSON                   |
| Producer ACK               | `all`                  |
| Producer Idempotence       | Enabled                |
| Producer Retries           | `5`                    |
| Consumer Auto Commit       | Disabled               |
| Consumer Offset Reset      | `earliest`             |
| Consumer Max Poll Records  | `50`                   |
| Consumer Concurrency       | `3`                    |
| Listener Ack Mode          | `MANUAL_IMMEDIATE`     |
| Retry Type                 | Exponential Backoff    |
| Initial Retry Interval     | `1000 ms`              |
| Retry Multiplier           | `2.0`                  |
| Retry Maximum Elapsed Time | `15000 ms`             |
| DLT                        | `order-processing-dlt` |
| Main Topic Partitions      | `3`                    |
| Topic Replication          | `1` for local setup    |
| Message Key                | `orderId`              |

---

# 40. Important Configuration Files

The main Kafka configuration files used in the implementation are:

### Order Service

```text
order-service/src/main/java/com/example/orderservice/config/KafkaProducerConfig.java

order-service/src/main/java/com/example/orderservice/config/KafkaConsumerConfig.java

order-service/src/main/java/com/example/orderservice/config/KafkaTopicConfig.java
```

### Payment Service

```text
payment-service/src/main/java/com/example/paymentservice/config/KafkaProducerConfig.java

payment-service/src/main/java/com/example/paymentservice/config/KafkaConsumerConfig.java

payment-service/src/main/java/com/example/paymentservice/config/KafkaTopicConfig.java
```

### Notification Service

```text
notification-service/src/main/java/com/example/notificationservice/config/KafkaProducerConfig.java

notification-service/src/main/java/com/example/notificationservice/config/KafkaConsumerConfig.java

notification-service/src/main/java/com/example/notificationservice/config/KafkaTopicConfig.java
```

---

# 41. Implementation Files for Reference

## Order Service

```text
OrderController.java
OrderService.java
OrderStatusUpdaterConsumer.java
OrderCreatedEvent.java
PaymentCompletedEvent.java
PaymentFailedEvent.java
```

## Payment Service

```text
PaymentConsumer.java
PaymentService.java
OrderCreatedEvent.java
PaymentCompletedEvent.java
PaymentFailedEvent.java
TransientPaymentException.java
```

## Notification Service

```text
NotificationConsumer.java
AuditConsumer.java
DltMonitorConsumer.java
NotificationEvent.java
PaymentCompletedEvent.java
PaymentFailedEvent.java
```

---

# 42. Key Kafka Concepts Demonstrated

This implementation demonstrates the following Kafka concepts:

* Kafka Producer
* Kafka Consumer
* Kafka Topics
* Kafka Partitions
* Consumer Groups
* Message Keys
* Partition Distribution
* JSON Serialization
* JSON Deserialization
* Producer Acknowledgements
* Producer Retries
* Idempotent Producer
* Manual Offset Commit
* Consumer Concurrency
* Asynchronous Processing
* Exponential Backoff
* Error Handling
* Dead Letter Topic
* Consumer Failure Handling
* Consumer Recovery
* Kafka Rebalancing
* End-to-End Event Flow
* Logging and Monitoring

---

# 43. Final End-to-End Demonstration

For the final demonstration, perform the following sequence:

```text
1. Start Kafka.

2. Start Order Service.

3. Start Payment Service.

4. Start Notification Service.

5. Open Kafka UI.

6. Verify:
   - order-created
   - payment-completed
   - payment-failed
   - notification-events
   - order-processing-dlt

7. Verify each topic has 3 partitions.

8. Verify consumer groups.

9. Send a successful order using Postman.

10. Show order-created message.

11. Show Payment Service consuming the message.

12. Show payment-completed message.

13. Show Notification Service consuming the payment event.

14. Show notification-events message.

15. Check the order status using GET API.

16. Send amount 6000.

17. Demonstrate payment-failed flow.

18. Send amount 9999.

19. Demonstrate retry and exponential backoff.

20. Show the failed message in the DLT.

21. Show partition, offset and key in the logs.

22. Demonstrate consumer restart/recovery.

23. Explain consumer-group and partition distribution.
```

---

# 44. Conclusion

The project demonstrates a complete asynchronous event-driven workflow:

```text
REST API
   ↓
Order Service
   ↓
order-created
   ↓
Payment Service
   ↓
 ┌───────────────────────┐
 │                       │
 ↓                       ↓
payment-completed    payment-failed
 │                       │
 └───────────┬───────────┘
             ↓
    Notification Service
             ↓
   notification-events
             ↓
       Audit Consumer
```

The implementation also demonstrates Kafka partitioning, consumer groups, message keys, manual offset management, JSON event communication, retry handling, DLT processing, failure handling, recovery behavior, asynchronous processing, and end-to-end event-driven communication.
