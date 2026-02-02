package ru.yandex.practicum.commerce.delivery.service;

import java.math.BigDecimal;
import java.util.UUID;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

/**
 * Service interface for managing simulation delivery operations.
 */
public interface DeliveryService {

    /**
     * Created a new Delivery REcord to the DB.
     */
    DeliveryDto planDelivery(DeliveryDto delivery);

    /**
     * Updates the delivery status to {@code DELIVERED} and notifies the order service to update the
     * order status accordingly.
     */
    void confirmDelivery(UUID orderId);

    /**
     * Updates the delivery status to {@code IN_PROGRESS } and calls the order service to mark the
     * order as {@code ASSEMBLED}.
     */
    void acceptForDelivery(UUID orderId);

    /**
     * Updates the delivery status to {@code FAILED} and notifies the order service to mark the order
     * as {@code DELIVERY_FAILED}.
     */
    void failDelivery(UUID orderId);

    /**
     * Calculates the delivery cost for the given order.
     * <p>
     * The cost depends on:
     * <ul>
     *     <li>Warehouse address (affects base multiplier)</li>
     *     <li>Delivery destination street</li>
     *     <li>Fragility of goods</li>
     *     <li>Order weight and volume</li>
     * </ul>
     *
     * @param orderDto the order details
     * @return calculated delivery cost
     */
    BigDecimal calculateDeliveryCost(OrderDto orderDto);
}
