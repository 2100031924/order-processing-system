package com.example.paymentservice.exception;

public class TransientPaymentException extends RuntimeException {
    public TransientPaymentException(String message) {
        super(message);
    }
}
