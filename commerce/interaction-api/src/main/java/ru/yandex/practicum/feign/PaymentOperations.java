package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

/**
 * Client API interface for managing payment operations. Acting as a payment gateway adapter.
 * Simulates payment processing for an order, including calculation of cost and emulation of payment
 * status updates without connecting to a real payment provider.
 */
@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentOperations {

    /**
     * Creates a payment record for the given order to store all relevant pricing information.
     * @param orderDto the order to generate a payment for
     * @return the created payment object with its calculated fields and status
     */
    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    PaymentDto createPayment(@RequestBody @Valid OrderDto orderDto);

    /**
     * Calculates the full cost of the order including products, delivery, and tax.
     *
     * @param orderDto the order with known delivery  and product prices
     * @return the full total price for the order
     */
    @PostMapping("/totalCost")
    @ResponseStatus(HttpStatus.OK)
    BigDecimal calculateTotalCost(@RequestBody @Valid OrderDto orderDto);

    /**
     * Simulates a successful payment operation.
     * @param paymentId the UUID of the payment to mark as successful
     */
    @PostMapping("/refund")
    @ResponseStatus(HttpStatus.OK)
    void confirmPayment(@RequestBody UUID paymentId);

    /**
     * Calculates the total cost of products in the order.
     * @param orderDto the order containing product IDs and quantities
     * @return the total product cost
     */
    @PostMapping("/productCost")
    @ResponseStatus(HttpStatus.OK)
    BigDecimal calculateProductCost(@RequestBody @Valid OrderDto orderDto);

    /**
     * Simulates a failed payment operation.
     * @param paymentId the UUID of the payment to mark as failed
     */
    @PostMapping("/failed")
    @ResponseStatus(HttpStatus.OK)
    void markPaymentFailed(@RequestBody UUID paymentId);

}

