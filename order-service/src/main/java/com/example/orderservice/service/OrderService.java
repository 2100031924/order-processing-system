package com.example.orderservice.service;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.event.OrderCreatedEvent;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<String, Order> orderRepository = new ConcurrentHashMap<>();

    @Value("${app.kafka.topics.order-created}")
    private String orderCreatedTopic;

    public OrderResponse createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        Order order = Order.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .amount(request.getAmount())
                .status(OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderRepository.put(orderId, order);

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .amount(order.getAmount())
                .timestamp(LocalDateTime.now())
                .build();

        log.info("Dispatching OrderCreatedEvent for Order ID: {} with Key: {}", orderId, orderId);
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(orderCreatedTopic, orderId, event);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("OrderCreatedEvent successfully published: Order ID [{}] -> Topic [{}], Partition [{}], Offset [{}]",
                        orderId, result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish OrderCreatedEvent for Order ID: [{}]", orderId, ex);
            }
        });

        return OrderResponse.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .amount(order.getAmount())
                .status(order.getStatus())
                .message("Order submitted successfully and is processing asynchronously.")
                .build();
    }

    public Order getOrder(String orderId) {
        return orderRepository.get(orderId);
    }

    public List<Order> listOrders() {
        return orderRepository.values().stream()
                .sorted(Comparator.comparing(Order::getCreatedAt).reversed())
                .toList();
    }

    public void updateOrderStatus(String orderId, OrderStatus status) {
        Order order = orderRepository.get(orderId);
        if (order != null) {
            order.setStatus(status);
            orderRepository.put(orderId, order);
            log.info("In-memory order state updated: Order ID [{}] -> Status [{}]", orderId, status);
        }
    }
}
