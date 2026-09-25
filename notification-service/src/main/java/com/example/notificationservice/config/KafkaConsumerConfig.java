package com.example.notificationservice.config;

import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.event.PaymentCompletedEvent;
import com.example.notificationservice.event.PaymentFailedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.dead-letter-topic}")
    private String dltTopic;

    @Value("${app.kafka.retry.initial-interval-ms:1000}")
    private long retryInitialIntervalMs;

    @Value("${app.kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${app.kafka.retry.max-elapsed-time-ms:15000}")
    private long retryMaxElapsedTimeMs;

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
        return props;
    }

    private <T> ConsumerFactory<String, T> typedFactory(Class<T> targetType) {
        Map<String, Object> props = baseConsumerProps();
        JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(targetType);
        valueDeserializer.addTrustedPackages("com.example.notificationservice.event", "java.util", "java.lang");
        valueDeserializer.setUseTypeHeaders(true);
        valueDeserializer.setRemoveTypeHeaders(false);
        ErrorHandlingDeserializer<String> keyDeserializer = new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<T> valDeserializer = new ErrorHandlingDeserializer<>(valueDeserializer);
        return new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valDeserializer);
    }

    @Bean
    public ConsumerFactory<String, PaymentCompletedEvent> paymentCompletedConsumerFactory() {
        Map<String, Object> props = baseConsumerProps();
        JsonDeserializer<PaymentCompletedEvent> valueDeserializer = new JsonDeserializer<>(PaymentCompletedEvent.class);
        valueDeserializer.addTrustedPackages("com.example.notificationservice.event", "java.util", "java.lang");
        valueDeserializer.setUseTypeHeaders(true);
        valueDeserializer.setRemoveTypeHeaders(false);
        ErrorHandlingDeserializer<String> keyDeserializer = new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<PaymentCompletedEvent> valDeserializer = new ErrorHandlingDeserializer<>(valueDeserializer);
        return new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valDeserializer);
    }

    @Bean
    public ConsumerFactory<String, PaymentFailedEvent> paymentFailedConsumerFactory() {
        return typedFactory(PaymentFailedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, NotificationEvent> notificationEventConsumerFactory() {
        return typedFactory(NotificationEvent.class);
    }

    @Bean
    public ConsumerFactory<String, String> dltRawConsumerFactory() {
        Map<String, Object> props = baseConsumerProps();
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public DefaultErrorHandler notificationErrorHandler(KafkaOperations<String, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> {
                    log.error("Retries exhausted for key [{}] on topic [{}]. Routing to DLT [{}].", record.key(), record.topic(), dltTopic, exception);
                    Header originalTopicHeader = record.headers().lastHeader("kafka_dlt-original-topic");
                    String original = originalTopicHeader != null ? new String(originalTopicHeader.value()) : record.topic();
                    log.error("Original topic was [{}] for key [{}].", original, record.key());
                    return new TopicPartition(dltTopic, record.partition());
                });
        ExponentialBackOff backOff = new ExponentialBackOff(retryInitialIntervalMs, retryMultiplier);
        backOff.setMaxElapsedTime(retryMaxElapsedTimeMs);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    @Bean
    public DefaultErrorHandler dltMonitorErrorHandler() {
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        return new DefaultErrorHandler(backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> paymentCompletedKafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentCompletedEvent> paymentCompletedConsumerFactory,
            DefaultErrorHandler notificationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentCompletedConsumerFactory);
        factory.setCommonErrorHandler(notificationErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent> paymentFailedKafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentFailedEvent> paymentFailedConsumerFactory,
            DefaultErrorHandler notificationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentFailedConsumerFactory);
        factory.setCommonErrorHandler(notificationErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> notificationEventKafkaListenerContainerFactory(
            ConsumerFactory<String, NotificationEvent> notificationEventConsumerFactory,
            DefaultErrorHandler notificationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(notificationEventConsumerFactory);
        factory.setCommonErrorHandler(notificationErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dltRawKafkaListenerContainerFactory(
            ConsumerFactory<String, String> dltRawConsumerFactory,
            DefaultErrorHandler dltMonitorErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dltRawConsumerFactory);
        factory.setCommonErrorHandler(dltMonitorErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1);
        return factory;
    }
}
