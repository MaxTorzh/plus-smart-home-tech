package ru.yandex.practicum.commerce.payment.controller;

import java.math.BigDecimal;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.payment.service.PaymentService;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.feign.PaymentOperations;

/**
 * REST controller for managing payment operations in the online store.
 * Implements {@link PaymentOperations}.
 */
@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
@Slf4j
@Validated
public class PaymentController implements PaymentOperations {

    private final PaymentService paymentService;

    /**
     * Creates a payment record for the given order and stores all relevant pricing information.
     * @param orderDto the order to generate a payment for
     * @return the created payment object with its calculated fields and status
     */
    @Override
    public PaymentDto createPayment(final OrderDto orderDto) {
        log.info("Received request to create a payment for order with ID {}.", orderDto.getOrderId());
        final PaymentDto payment = paymentService.createPayment(orderDto);
        log.info("Returning created payment: {}.", payment.getPaymentId());
        return payment;
    }

    /**
     * Calculates the full cost of the order including products, delivery, and tax.
     * @param order the order with known delivery cost and products cost
     * @return the full total price for the order
     */
    @Override
    public BigDecimal calculateTotalCost(final OrderDto order) {
        log.info("Received request to calculate total cost for the order {}.", order.getOrderId());
        final BigDecimal total = paymentService.calculateTotalCost(order);
        log.info("Returning calculated total cost for the order {}: {}.", order.getOrderId(), total);
        return total;
    }

    /**
     * Calculates the total cost of products in the order.
     * @param order the order containing product IDs and quantities
     * @return the total product cost
     */
    @Override
    public BigDecimal calculateProductCost(final OrderDto order) {
        log.info("Received request to calculate products cost for the order {}.", order.getOrderId());
        final BigDecimal productCost = paymentService.calculateProductCost(order);
        log.info("Returning product cost calculated for the order {}: {}.", order.getOrderId(), productCost);
        return productCost;
    }

    /**
     * Simulates a successful payment operation.
     * @param paymentId the UUID of the payment to mark as successful
     */
    @Override
    public void confirmPayment(final UUID paymentId) {
        log.info("Received request to confirm successful payment with id {}.", paymentId);
        paymentService.confirmPayment(paymentId);
        log.info("Payment {} marked as SUCCESS.", paymentId);
    }

    /**
     * Simulates a failed payment operation.
     * @param paymentId the UUID of the payment to mark as failed
     */
    @Override
    public void markPaymentFailed(final UUID paymentId) {
        log.info("Received request to fail payment with id {}.", paymentId);
        paymentService.failPayment(paymentId);
        log.info("Payment {} marked as FAILED.", paymentId);
    }
}
