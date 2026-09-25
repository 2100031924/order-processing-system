package com.example.orderservice.consumer;

import com.example.orderservice.event.PaymentCompletedEvent;
import com.example.orderservice.event.PaymentFailedEvent;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderStatusUpdaterConsumer {
    private final OrderService orderService;

    @KafkaListener(
            topics = {"${app.kafka.topics.payment-completed}", "${app.kafka.topics.payment-failed}"},
            groupId = "${app.kafka.consumer-groups.order-status-updater}",
            containerFactory = "kafkaListenerContainerFactory")
    public void handlePaymentResult(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        log.info("OrderStatusUpdaterConsumer consumed record from topic [{}] Key [{}] Partition [{}] Offset [{}] Thread [{}]",
                record.topic(), record.key(), record.partition(), record.offset(), Thread.currentThread().getName());

        Object event = record.value();
        if (event == null) {
            log.warn("Null payload at partition [{}] offset [{}]. Committing and skipping.", record.partition(), record.offset());
            ack.acknowledge();
            return;
        }

        if (event instanceof PaymentCompletedEvent completed) {
            orderService.updateOrderStatus(completed.getOrderId(), OrderStatus.PAYMENT_SUCCESSFUL);
            log.info("Order [{}] marked as PAYMENT_SUCCESSFUL", completed.getOrderId());
        } else if (event instanceof PaymentFailedEvent failed) {
            orderService.updateOrderStatus(failed.getOrderId(), OrderStatus.PAYMENT_FAILED);
            log.info("Order [{}] marked as PAYMENT_FAILED with reason [{}]", failed.getOrderId(), failed.getFailureReason());
        } else {
            throw new IllegalArgumentException("Unexpected event type: " + event.getClass().getName());
        }

        ack.acknowledge();
        log.info("Offset committed for partition [{}] at offset [{}]", record.partition(), record.offset());
    }
}
