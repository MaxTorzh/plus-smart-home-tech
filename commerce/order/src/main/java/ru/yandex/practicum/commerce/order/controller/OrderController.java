package ru.yandex.practicum.commerce.order.controller;

import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.order.service.OrderService;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.OrderState;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.feign.OrderOperations;

/**
 * REST controller for managing orders in the online store.
 * <p>
 * Implements {@link OrderOperations}.
 */
@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
@Slf4j
@Validated
public class OrderController implements OrderOperations {

    private final OrderService orderService;

    @Override
    public List<OrderDto> getUserOrders(final String username) {
        log.info("Received request to view orders associated with user {}", username);
        final List<OrderDto> orders = orderService.getOrdersByUser(username);
        log.info("Returning list of {} orders for user {}", orders.size(), username);
        return orders;
    }

    @Override
    public OrderDto createNewOrder(final CreateNewOrderRequest request, final String username) {
        log.info("Received request to create new order from shopping cart {} for user {}",
                request.getShoppingCart().getShoppingCartId(), username);
        final OrderDto createdOrder = orderService.createOrder(request, username);
        log.info("New Order created with ID {}", createdOrder.getOrderId());
        return createdOrder;
    }

    @Override
    public OrderDto returnProduct(final ProductReturnRequest request) {
        log.info("Received request to return products.");
        final OrderDto order = orderService.returnOrder(request);
        log.info("Returning updated order.");
        return order;
    }

    @Override
    public OrderDto processPayment(final UUID orderId) {
        log.info("Received request to process payment success for order {}.", orderId);
        final OrderDto order = orderService.processPaymentSuccess(orderId);
        log.info("Order with ID {} marked as PAID.", order.getOrderId());
        return order;
    }

    @Override
    public OrderDto markPaymentFailed(final UUID orderId) {
        log.info("Received request to mark payment FAILED for order {}.", orderId);
        final OrderDto order = orderService.updateOrderState(orderId, OrderState.PAYMENT_FAILED);
        log.info("Order with ID {} marked with PAYMENT_FAILED.", order.getOrderId());
        return order;
    }

    @Override
    public OrderDto markAsDelivered(final UUID orderId) {
        log.info("Received request to mark order {} as DELIVERED.", orderId);
        final OrderDto order = orderService.updateOrderState(orderId, OrderState.DELIVERED);
        log.info("Order with ID {} marked as DELIVERED.", order.getOrderId());
        return order;
    }

    @Override
    public OrderDto markDeliveryFailed(final UUID orderId) {
        log.info("Received request to mark order {} with DELIVERY_FAILED.", orderId);
        final OrderDto order = orderService.updateOrderState(orderId, OrderState.DELIVERY_FAILED);
        log.info("Order with ID {} marked with DELIVERY_FAILED.", order.getOrderId());
        return order;
    }

    @Override
    public OrderDto complete(final UUID orderId) {
        log.info("Received request to mark order {} as COMPLETED.", orderId);
        final OrderDto order = orderService.updateOrderState(orderId, OrderState.COMPLETED);
        log.info("Order with ID {} marked as COMPLETED.", order.getOrderId());
        return order;
    }

    @Override
    public OrderDto calculateDeliveryCost(final UUID orderId) {
        log.info("Received request to calculate delivery cost for the order with ID {}.", orderId);
        final OrderDto order = orderService.calculateDeliveryCost(orderId);
        log.info("Returning order info with delivery cost calculated: {}.", order.getDeliveryPrice());
        return order;
    }

    @Override
    public OrderDto calculateTotalCost(final UUID orderId) {
        log.info("Received request to calculate total cost for the order with ID {}.", orderId);
        final OrderDto order = orderService.calculateTotalCost(orderId);
        log.info("Returning updated order info with total cost calculated: {}.", order.getTotalPrice());
        return order;
    }

    @Override
    public OrderDto assembleOrder(final UUID orderId) {
        log.info("Received request to mark order {} as ASSEMBLED.", orderId);
        OrderDto order = orderService.updateOrderState(orderId, OrderState.ASSEMBLED);
        log.info("Returning updated order marked as ASSEMBLED.");
        return order;
    }

    @Override
    public OrderDto markAssemblyFailed(final UUID orderId) {
        log.info("Received request to mark order {} with ASSEMBLY_FAILED.", orderId);
        final OrderDto order = orderService.updateOrderState(orderId, OrderState.ASSEMBLY_FAILED);
        log.info("Order with ID {} marked with ASSEMBLY_FAILED.", order.getOrderId());
        return order;
    }
}
