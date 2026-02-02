package ru.yandex.practicum.commerce.order.mapper;

import java.util.List;
import java.util.Objects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;

/**
 * Utility class for mapping between {@link Order} and {@link OrderDto} objects.
 */
@UtilityClass
@Slf4j
public class OrderMapper {

    public  OrderDto toDto(final Order order) {
        Objects.requireNonNull(order);
        return OrderDto.builder()
                .orderId(order.getOrderId())
                .shoppingCartId(order.getShoppingCartId())
                .products(order.getProducts())
                .paymentId(order.getPaymentId())
                .deliveryId(order.getDeliveryId())
                .state(order.getState())
                .deliveryWeight(order.getDeliveryWeight())
                .deliveryVolume(order.getDeliveryVolume())
                .fragile(order.isFragile())
                .totalPrice(order.getTotalPrice())
                .deliveryPrice(order.getDeliveryPrice())
                .productPrice(order.getProductPrice())
                .build();
    }

    public  List<OrderDto> toDto(final List<Order> orders) {
        Objects.requireNonNull(orders);
        return orders.stream()
                .map(OrderMapper::toDto)
                .toList();
    }

    public  AssemblyProductsForOrderRequest toAssembleRequest(final Order order) {
        Objects.requireNonNull(order);
        return AssemblyProductsForOrderRequest.builder()
                .orderId(order.getOrderId())
                .products(order.getProducts())
                .build();
    }
}
