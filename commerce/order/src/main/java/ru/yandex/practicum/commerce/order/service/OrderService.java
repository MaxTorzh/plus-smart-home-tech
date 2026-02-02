package ru.yandex.practicum.commerce.order.service;

import java.util.List;
import java.util.UUID;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.OrderState;
import ru.yandex.practicum.dto.order.ProductReturnRequest;

/**
 * Service interface for managing product orders.
 */
public interface OrderService {

    List<OrderDto> getOrdersByUser(String username);

    OrderDto createOrder(CreateNewOrderRequest request, String username);

    OrderDto returnOrder(ProductReturnRequest request);

    OrderDto updateOrderState(UUID orderId, OrderState orderState);

    OrderDto calculateDeliveryCost(UUID orderId);

    OrderDto calculateTotalCost(UUID orderId);

    OrderDto processPaymentSuccess(UUID orderId);
}

