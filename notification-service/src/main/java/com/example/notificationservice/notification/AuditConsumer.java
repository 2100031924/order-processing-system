package com.example.notificationservice.notification;

import com.example.notificationservice.event.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AuditConsumer {
    @KafkaListener(
            topics = "${app.kafka.topics.notification-events}",
            groupId = "${app.kafka.consumer-groups.audit}",
            containerFactory = "notificationEventKafkaListenerContainerFactory")
    public void onNotificationEvent(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();
        if (event == null) {
            log.warn("Null NotificationEvent at partition [{}] offset [{}]. Committing and skipping.", record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
        log.info("AUDIT LOG -> Order ID: [{}], Recipient: [{}], Status: [{}], Message: [{}], Partition: [{}], Offset: [{}]", event.getOrderId(), event.getRecipient(), event.getStatus(), event.getMessage(), record.partition(), record.offset());
        ack.acknowledge();
    }
}
