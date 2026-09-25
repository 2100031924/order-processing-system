package com.example.notificationservice.notification;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class DltMonitorConsumer {
    @KafkaListener(
            topics = "${app.kafka.topics.dead-letter-topic}",
            groupId = "${app.kafka.consumer-groups.dlt-monitor}",
            containerFactory = "dltRawKafkaListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        Header header = record.headers().lastHeader("kafka_dlt-original-topic");
        String originalTopic = header != null ? new String(header.value(), StandardCharsets.UTF_8) : "unknown";
        Header exceptionHeader = record.headers().lastHeader("kafka_dlt-exception-message");
        String exceptionMessage = exceptionHeader != null ? new String(exceptionHeader.value(), StandardCharsets.UTF_8) : "unknown";
        log.error("DLT MONITOR -> original topic [{}], key [{}], partition [{}], offset [{}], exception [{}], value [{}]", originalTopic, record.key(), record.partition(), record.offset(), exceptionMessage, record.value());
        ack.acknowledge();
    }
}
