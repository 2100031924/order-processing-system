package com.example.notificationservice.notification;

import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.event.PaymentCompletedEvent;
import com.example.notificationservice.event.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {
    private static final long SEND_TIMEOUT_SECONDS = 10;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.notification-events}")
    private String notificationEventsTopic;

    @KafkaListener(topics = "${app.kafka.topics.payment-completed}",
            groupId = "${app.kafka.consumer-groups.notification}",
            containerFactory = "paymentCompletedKafkaListenerContainerFactory")
    public void onPaymentCompleted(ConsumerRecord<String, PaymentCompletedEvent> record, Acknowledgment ack) {
        PaymentCompletedEvent event = record.value();
        if (event == null) {
            log.warn("Null PaymentCompletedEvent at partition [{}] offset [{}]. Committing and skipping.", record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
        log.info("NotificationConsumer consumed PaymentCompletedEvent for order [{}] from partition [{}] offset [{}].", event.getOrderId(), record.partition(), record.offset());
        NotificationEvent notification = NotificationEvent.builder()
                .notificationId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .recipient("customer_" + event.getCustomerId() + "@domain.com")
                .message("Payment of " + event.getAmount() + " succeeded. Your order is being prepared.")
                .status("SENT")
                .sentAt(LocalDateTime.now())
                .build();
        publishNotification(event.getOrderId(), notification);
        ack.acknowledge();
        log.info("Notification for order [{}] published and offset committed.", event.getOrderId());
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-failed}",
            groupId = "${app.kafka.consumer-groups.notification}",
            containerFactory = "paymentFailedKafkaListenerContainerFactory")
    public void onPaymentFailed(ConsumerRecord<String, PaymentFailedEvent> record, Acknowledgment ack) {
        PaymentFailedEvent event = record.value();
        if (event == null) {
            log.warn("Null PaymentFailedEvent at partition [{}] offset [{}]. Committing and skipping.", record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
        log.info("NotificationConsumer consumed PaymentFailedEvent for order [{}] from partition [{}] offset [{}].", event.getOrderId(), record.partition(), record.offset());
        NotificationEvent notification = NotificationEvent.builder()
                .notificationId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .recipient("customer_" + event.getCustomerId() + "@domain.com")
                .message("Payment of $" + event.getAmount() + " failed: " + event.getFailureReason())
                .status("FAILED_ALERT_SENT")
                .sentAt(LocalDateTime.now())
                .build();
        publishNotification(event.getOrderId(), notification);
        ack.acknowledge();
        log.info("Failure notification for order [{}] published and offset committed.", event.getOrderId());
    }

    private void publishNotification(String orderId, NotificationEvent notification) {
        try {
            SendResult<String, Object> result = kafkaTemplate.send(notificationEventsTopic, orderId, notification).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("NotificationEvent published for order [{}] to partition [{}] offset [{}].", orderId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing NotificationEvent for order " + orderId, ie);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Broker did not acknowledge NotificationEvent for order " + orderId, ex);
        }
    }
}
