package ru.yandex.practicum.commerce.payment.service;

import java.math.BigDecimal;
import java.util.UUID;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

/**
 * Service interface for managing payment operation simulation.
 */
public interface PaymentService {

    PaymentDto createPayment(OrderDto orderDto);

    BigDecimal calculateTotalCost(OrderDto orderDto);

    BigDecimal calculateProductCost(OrderDto order);

    void confirmPayment(UUID paymentId);

    void failPayment(UUID paymentId);
}