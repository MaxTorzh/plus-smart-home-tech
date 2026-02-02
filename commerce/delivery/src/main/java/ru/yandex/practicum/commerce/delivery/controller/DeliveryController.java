package ru.yandex.practicum.commerce.delivery.controller;

import java.math.BigDecimal;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.feign.DeliveryOperations;

/**
 * REST controller for managing delivery operations in the online store.
 * Implements {@link DeliveryOperations}.
 */
@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
@Slf4j
@Validated
public class DeliveryController implements DeliveryOperations {

    private final DeliveryService deliveryService;

    /**
     * Registers a new delivery in the system.
     *
     * @param delivery delivery information to be saved
     * @return saved delivery with assigned ID
     */
    @Override
    public DeliveryDto planDelivery(final DeliveryDto delivery) {
        log.info("Received request to plan a new delivery for the order ID {}.", delivery.getDeliveryId());
        final DeliveryDto savedDelivery = deliveryService.planDelivery(delivery);
        log.info("Delivery saved successfully with delivery ID {}.",savedDelivery.getDeliveryId());
        return savedDelivery;
    }

    /**
     * Simulates a successful delivery confirmation.
     *
     * @param orderId the ID of the associated order
     * @return HTTP 200 if successful, 404 if no delivery was found
     */
    @Override
    public void confirmDelivery(final UUID orderId) {
        log.info("Received request to confirm successful delivery of order with ID {}.", orderId);
        deliveryService.confirmDelivery(orderId);
        log.info("Order delivery confirmed successfully.");
    }

    /**
     * Simulates the event of order being picked up for delivery.
     *
     * @param orderId the ID of the associated order
     */
    @Override
    public void markDeliveryPicked(final UUID orderId) {
        log.info("Marking delivery as picked for order {}", orderId);
        deliveryService.acceptForDelivery(orderId);
        log.info("Delivery accepted and in progress for order {}", orderId);
    }

    /**
     * Simulates a delivery failure event.
     *
     * @param orderId the ID of the associated order
     */
    @Override
    public void markDeliveryFailed(final UUID orderId) {
        log.info("Received request to mark delivery as FAILED fo order {}.", orderId);
        deliveryService.failDelivery(orderId);
        log.info("Delivery marked as FAILED.");
    }

    /**
     * Calculates the delivery cost for the given order.
     *
     * @param orderDto the order details
     * @return calculated delivery cost
     */
    @Override
    public BigDecimal calculateDeliveryCost(final OrderDto orderDto) {
        log.info("Received request to calculate delivery cost for the order {}.", orderDto.getOrderId());
        final BigDecimal deliveryCost = deliveryService.calculateDeliveryCost(orderDto);
        log.info("Delivery cost calculated for the order with ID {}: {} .", deliveryCost, orderDto.getOrderId());
        return deliveryCost;
    }
}

