package ru.yandex.practicum.commerce.payment.service;

import java.math.BigDecimal;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;
import ru.yandex.practicum.commerce.payment.utility.UuidGenerator;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.payment.PaymentStatus;
import ru.yandex.practicum.exception.NoPaymentFoundException;
import ru.yandex.practicum.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.exception.PaymentForSpecifiedOrderAlreadyExists;
import ru.yandex.practicum.feign.OrderOperations;

/**
 * Service implementation of {@link PaymentService}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final UuidGenerator uuidGenerator;
    private final PaymentCalculator paymentCalculator;
    private final OrderOperations orderClient;

    @Override
    @Transactional
    public PaymentDto createPayment(final OrderDto order) {
        log.debug("Creating a new payment for the order {}.", order.getOrderId());

        validatePaymentDoesNotExists(order.getOrderId());

        final Payment paymentToCreate = buildPayment(order);
        final Payment created = paymentRepository.save(paymentToCreate);

        return PaymentMapper.toDto(created);
    }

    @Override
    public BigDecimal calculateTotalCost(final OrderDto order) {
        log.debug("Calculating total cost of the order {}.", order);

        validateDeliveryCostPresent(order);
        validateProductCostPresent(order);

        return paymentCalculator.calculateTotal(order.getDeliveryPrice(), order.getProductPrice());
    }

    @Override
    public BigDecimal calculateProductCost(final OrderDto order) {
        log.debug("Calculating product cost of the order {}.", order);
        validateProductsPresent(order);
        return paymentCalculator.calculateProductCost(order.getProducts());
    }

    @Override
    @Transactional
    public void confirmPayment(final UUID paymentId) {
        log.debug("Marking payment with status SUCCESS.");
        final Payment payment = updatePaymentStatus(paymentId, PaymentStatus.SUCCESS);
        orderClient.processPayment(payment.getOrderId());
    }

    @Override
    @Transactional
    public void failPayment(final UUID paymentId) {
        log.debug("Marking payment as fail: {}.", paymentId);
        final Payment payment = updatePaymentStatus(paymentId, PaymentStatus.FAILED);
        orderClient.markPaymentFailed(payment.getOrderId());
    }

    private Payment updatePaymentStatus(final UUID paymentId, final PaymentStatus newState) {

        final Payment payment = getPaymentOrThrow(paymentId);
        payment.setStatus(newState);
        return paymentRepository.save(payment);
    }

    private Payment getPaymentOrThrow(final UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoPaymentFoundException("Payment ID: " + paymentId));
    }

    private void validateProductsPresent(final OrderDto order) {
        if (order.getProducts() == null || order.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateException("Order ID " + order.getOrderId());
        }
    }

    private void validateProductCostPresent(final OrderDto order) {
        if (order.getProductPrice() == null || order.getDeliveryPrice().equals(BigDecimal.ZERO)) {
            throw new NotEnoughInfoInOrderToCalculateException("Order ID " + order.getOrderId());
        }
    }

    private void validateDeliveryCostPresent(final OrderDto order) {
        if (order.getDeliveryPrice() == null || order.getDeliveryPrice().equals(BigDecimal.ZERO)) {
            throw new NotEnoughInfoInOrderToCalculateException("Order ID " + order.getOrderId());
        }
    }

    private Payment buildPayment(final OrderDto order) {
        log.debug("Building Payment out od OrderDto {}.", order);
        return Payment.builder()
                .paymentId(uuidGenerator.generate())
                .orderId(order.getOrderId())
                .deliveryTotal(order.getDeliveryPrice())
                .totalPayment(order.getTotalPrice())
                .feeTotal(order.getTotalPrice().subtract(order.getDeliveryPrice()))
                .status(PaymentStatus.PENDING)
                .build();
    }

    private void validatePaymentDoesNotExists(final UUID orderId) {
        log.debug("Validating payment for order ID {} does not exists.", orderId);
        if (paymentRepository.existsByOrderId(orderId)) {
            throw new PaymentForSpecifiedOrderAlreadyExists("Order ID: " + orderId);
        }
    }
}