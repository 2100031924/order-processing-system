package com.example.paymentservice.payment;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.event.PaymentCompletedEvent;
import com.example.paymentservice.event.PaymentFailedEvent;
import com.example.paymentservice.exception.TransientPaymentException;
import com.example.paymentservice.model.PaymentStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private static final BigDecimal RETRY_TRIGGER_AMOUNT = new BigDecimal("9999.00");
    private static final BigDecimal MAX_ALLOWED_AMOUNT = new BigDecimal("5000.00");
    private static final long SEND_TIMEOUT_SECONDS = 10;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    @Value("${app.kafka.topics.payment-completed}")
    private String paymentCompletedTopic;

    @Value("${app.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    public boolean isAlreadyProcessed(String eventId) {
        return eventId != null && processedEventIds.contains(eventId);
    }

    public PaymentStatus processPayment(OrderCreatedEvent event) {
        log.info("Executing payment check -> Order ID: [{}], Amount: [${}]",
                event.getOrderId(), event.getAmount());
        if (event.getAmount().compareTo(RETRY_TRIGGER_AMOUNT) == 0) {
            log.warn("Simulating 3rd-party payment gateway timeout for Order ID: [{}]. The consumer container will retry with exponential back-off.", event.getOrderId());
            throw new TransientPaymentException("Payment gateway timeout (simulated) - retryable error");
        }
        if (event.getAmount().compareTo(MAX_ALLOWED_AMOUNT) > 0) {
            log.warn("Payment rejected for Order ID: [{}]. Amount exceeds the ${} limit.",
                    event.getOrderId(), MAX_ALLOWED_AMOUNT);
            publishPaymentFailed(event,
                    "Transaction limit exceeded: max allowable amount is $" + MAX_ALLOWED_AMOUNT);
            processedEventIds.add(event.getEventId());
            return PaymentStatus.FAILED;
        }
        publishPaymentCompleted(event);
        processedEventIds.add(event.getEventId());
        return PaymentStatus.SUCCESS;
    }

    private void publishPaymentCompleted(OrderCreatedEvent event) {
        PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                .paymentId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .timestamp(LocalDateTime.now())
                .build();
        sendBlocking(paymentCompletedTopic, event.getOrderId(), completedEvent, "PaymentCompletedEvent");
    }

    private void publishPaymentFailed(OrderCreatedEvent event, String reason) {
        PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                .paymentId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .failureReason(reason)
                .timestamp(LocalDateTime.now())
                .build();
        sendBlocking(paymentFailedTopic, event.getOrderId(), failedEvent, "PaymentFailedEvent");
    }

    private void sendBlocking(String topic, String key, Object payload, String eventType) {
        try {
            SendResult<String, Object> result = kafkaTemplate.send(topic, key, payload)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("{} published -> Topic: [{}], Partition: [{}], Offset: [{}], Key: [{}]",
                    eventType,
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    key);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + eventType + " for order " + key, ie);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Broker did not acknowledge " + eventType + " for order " + key, ex);
        }
    }
}
