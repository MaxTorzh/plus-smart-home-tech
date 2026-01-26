package ru.yandex.practicum.commerce.order.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.order.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;
import ru.yandex.practicum.commerce.order.util.UuidGenerator;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.OrderState;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.feign.DeliveryOperations;
import ru.yandex.practicum.feign.PaymentOperations;
import ru.yandex.practicum.feign.WarehouseOperations;

/**
 * Service implementation for managing orders.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final WarehouseOperations warehouseClient;
    private final UuidGenerator uuidGenerator;
    private final DeliveryOperations deliveryClient;
    private final PaymentOperations paymentClient;

    @Transactional(readOnly = true)
    @Override
    public List<OrderDto> getOrdersByUser(final String username) {
        validateUser(username);
        log.debug("Fetching all orders that belongs user {}.", username);
        final List<Order> orders = orderRepository.findAllByUsername(username);
        return OrderMapper.toDto(orders);
    }

    @Transactional
    @Override
    public OrderDto createOrder(final CreateNewOrderRequest request, final String username) {
        validateUser(username);
        log.debug("Starting new order registration to the DB with data {} for user {}.",
                request, username);
        final UUID orderId = uuidGenerator.generate();
        final Order order = enrichOrderInfo(orderId, request, username);
        final Order savedOrder = orderRepository.save(order);
        log.debug("saved new order with ID {} for shopping cart {}",
                savedOrder.getOrderId(), savedOrder.getShoppingCartId());
        return OrderMapper.toDto(savedOrder);
    }

    @Transactional
    @Override
    public OrderDto returnOrder(final ProductReturnRequest request) {
        log.debug("Returning products {} from order {}.", request.getProducts(), request.getOrderId());
        final Order order = getOrderOrThrow(request.getOrderId());
        warehouseClient.acceptReturn(request.getProducts());
        final Order updatedOrder = persistOrderWithNewState(order, OrderState.PRODUCT_RETURNED);
        log.debug("Updated order after returning: {}.", updatedOrder);
        return OrderMapper.toDto(updatedOrder);
    }

    @Transactional
    @Override
    public OrderDto updateOrderState(final UUID orderId, final OrderState orderState) {
        final Order order = getOrderOrThrow(orderId);
        final Order updatedOrder = persistOrderWithNewState(order, orderState);
        return OrderMapper.toDto(updatedOrder);
    }

    @Transactional
    @Override
    public OrderDto calculateDeliveryCost(final UUID orderId) {
        final Order order = getOrderOrThrow(orderId);
        final BigDecimal deliveryCost = deliveryClient.calculateDeliveryCost(OrderMapper.toDto(order));
        order.setDeliveryPrice(deliveryCost);
        final Order updatedOrder = orderRepository.save(order);
        return OrderMapper.toDto(updatedOrder);
    }

    @Transactional
    @Override
    public OrderDto calculateTotalCost(final UUID orderId) {
        final Order order = getOrderOrThrow(orderId);
        final BigDecimal totalCost = paymentClient.calculateTotalCost(OrderMapper.toDto(order));
        order.setTotalPrice(totalCost);
        final Order updatedOrder = orderRepository.save(order);
        return OrderMapper.toDto(updatedOrder);
    }

    @Transactional
    @Override
    public OrderDto processPaymentSuccess(final UUID orderId) {
        log.debug("Starting processing succeed payment for order {}.", orderId);
        Order order = getOrderOrThrow(orderId);
        order.setState(OrderState.PAID);
        final BookedProductsDto bookedProducts = warehouseClient.assembleProductsForOrder(
                OrderMapper.toAssembleRequest(order));
        order.setDeliveryWeight(bookedProducts.getDeliveryWeight());
        order.setDeliveryVolume(bookedProducts.getDeliveryVolume());
        order.setFragile(bookedProducts.getFragile());
        final Order savedOrder = orderRepository.save(order);
        return OrderMapper.toDto(savedOrder);
    }

    private Order persistOrderWithNewState(final Order order, final OrderState state) {
        order.setState(state);
        return orderRepository.save(order);
    }

    private Order getOrderOrThrow(final UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order ID: " + orderId));
    }

    private BookedProductsDto checkProductsAvailability(final ShoppingCartDto shoppingCart) {
        log.debug("Sending request to the warehouse to check product availability: {}", shoppingCart);
        return warehouseClient.checkStock(shoppingCart);
    }

    private void validateUser(final String username) {
        log.debug("Validating username {}.", username);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Validation username " + username + " failed.");
        }
    }

    private Order enrichOrderInfo(final UUID orderId,
                                  final CreateNewOrderRequest request,
                                  final String username) {
        log.debug("Populating order fields with data, orderID {}, request {}, user {}.",
                orderId, request, username);
        final BookedProductsDto bookedProduct = checkProductsAvailability(request.getShoppingCart());
        final Order order = addDeliveryData(orderId, request, username, bookedProduct);
        addPaymentData(order);
        return order;
    }

    private void addPaymentData(final Order order) {
        log.debug("Calculating prices and creating payment for order {}", order.getOrderId());
        final OrderDto orderDto = OrderMapper.toDto(order);
        final BigDecimal deliveryCost = deliveryClient.calculateDeliveryCost(orderDto);
        log.debug("Calculated delivery cost: {}", deliveryCost);
        final BigDecimal productCost = paymentClient.calculateProductCost(orderDto);
        log.debug("Calculated product cost: {}", productCost);
        final BigDecimal totalCost = paymentClient.calculateTotalCost(orderDto);
        log.debug("Calculated total cost: {}", totalCost);
        final PaymentDto payment = paymentClient.createPayment(orderDto);
        log.debug("Created payment with ID {}", payment.getPaymentId());

        order.setDeliveryPrice(deliveryCost);
        order.setProductPrice(productCost);
        order.setTotalPrice(totalCost);
        order.setPaymentId(payment.getPaymentId());
    }

    private Order addDeliveryData(final UUID orderId,
                                  final CreateNewOrderRequest request,
                                  final String username,
                                  final BookedProductsDto bookedProducts) {
        log.debug("Fetching warehouse address...");
        final AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        final DeliveryDto deliveryRequest = DeliveryMapper.mapToDeliveryDto(orderId,
                request.getDeliveryAddress(), warehouseAddress);
        final DeliveryDto createdDelivery = deliveryClient.planDelivery(deliveryRequest);
        log.debug("Created delivery with ID {}", createdDelivery.getDeliveryId());
        return Order.builder()
                .orderId(orderId)
                .username(username)
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(request.getShoppingCart().getProducts())
                .state(OrderState.NEW)
                .deliveryId(createdDelivery.getDeliveryId())
                .deliveryWeight(bookedProducts.getDeliveryWeight())
                .deliveryVolume(bookedProducts.getDeliveryVolume())
                .fragile(bookedProducts.getFragile())
                .build();
    }
}