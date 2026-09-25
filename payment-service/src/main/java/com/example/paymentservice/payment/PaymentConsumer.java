package com.example.paymentservice.payment;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.model.PaymentStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${app.kafka.consumer-groups.payment}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeOrderCreated(
            ConsumerRecord<String, OrderCreatedEvent> record,
            Acknowledgment acknowledgment) {
        log.info("PaymentConsumer received record -> Key: [{}], Partition: [{}], Offset: [{}], Thread: [{}]",
                record.key(), record.partition(), record.offset(), Thread.currentThread().getName());
        OrderCreatedEvent event = record.value();
        if (event == null) {
            log.warn("Null payload at partition [{}] offset [{}]. Committing and skipping.",
                    record.partition(), record.offset());
            acknowledgment.acknowledge();
            return;
        }
        try {
            if (paymentService.isAlreadyProcessed(event.getEventId())) {
                log.warn("Duplicate delivery detected for eventId [{}] (order [{}]). Skipping re-processing.",
                        event.getEventId(), event.getOrderId());
                acknowledgment.acknowledge();
                return;
            }
            PaymentStatus paymentStatus = paymentService.processPayment(event);
            acknowledgment.acknowledge();
            log.info("Manual offset committed for Partition [{}] at Offset [{}] with payment status [{}]",
                    record.partition(), record.offset(), paymentStatus);
        } catch (Exception ex) {
            log.error("Error processing OrderCreatedEvent for key [{}]. Delegating to error handler.",
                    record.key(), ex);
            throw ex;
        }
    }
}
