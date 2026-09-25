package com.example.orderservice.dto;

import com.example.orderservice.model.OrderStatus;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class OrderResponse {
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private OrderStatus status;
    private String message;
}
