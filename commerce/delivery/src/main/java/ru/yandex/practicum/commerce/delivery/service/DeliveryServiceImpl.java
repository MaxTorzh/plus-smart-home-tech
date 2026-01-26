package ru.yandex.practicum.commerce.delivery.service;

import java.math.BigDecimal;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.delivery.util.DeliveryCostCalculator;
import ru.yandex.practicum.commerce.delivery.util.UuidGenerator;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.DeliveryState;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.exception.DeliveryForSpecifiedOrderAlreadyExists;
import ru.yandex.practicum.exception.NoDeliveryFoundException;
import ru.yandex.practicum.feign.OrderOperations;
import ru.yandex.practicum.feign.WarehouseOperations;

/**
 * Service implementation of {@link DeliveryService}
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final UuidGenerator uuidGenerator;
    private final OrderOperations orderClient;
    private final WarehouseOperations warehouseClient;
    private final DeliveryCostCalculator deliveryCostCalculator;

    @Override
    @Transactional
    public DeliveryDto planDelivery(final DeliveryDto deliveryDto) {
        log.debug("Creating new delivery with data: {}.", deliveryDto);

        validateDeliveryDoesNotExist(deliveryDto.getOrderId());

        final Delivery delivery = DeliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryId(uuidGenerator.generate());
        delivery.setDeliveryState(DeliveryState.CREATED);

        final Delivery savedDelivery = deliveryRepository.save(delivery);
        return DeliveryMapper.toDto(savedDelivery);
    }

    @Override
    @Transactional
    public void confirmDelivery(final UUID orderId) {
        log.debug("Setting delivery state for order with ID {} as DELIVERED.", orderId);

        final Delivery delivery = setDeliveryStateValue(orderId,DeliveryState.DELIVERED);

        log.debug("Sending request to Order service to confirm order {} is DELIVERED.", orderId);
        orderClient.markAsDelivered(delivery.getOrderId());
        log.debug("Delivery is confirmed in order service.");

        deliveryRepository.save(delivery);
    }

    @Override
    public void acceptForDelivery(final UUID orderId) {
        log.debug("Accepting order {} for delivery by updating its state as IN_PROGRESS.", orderId);

        final Delivery delivery = setDeliveryStateValue(orderId,DeliveryState.IN_PROGRESS);

        log.debug("Sending request to Order service, that order {} is assembled.", orderId);
        orderClient.assembleOrder(orderId);

        log.debug("Sending  Warehouse service delivery ID {}.", delivery.getDeliveryId());
        warehouseClient.sendToDelivery(DeliveryMapper.toShipRequest(orderId, delivery.getDeliveryId()));
        log.debug("Delivery {} for order {} marked as IN_PROGRESS and linked with warehouse.",
                delivery.getDeliveryId(), orderId);

        deliveryRepository.save(delivery);
    }

    @Override
    @Transactional
    public void failDelivery(final UUID orderId) {
        log.debug("Setting delivery state for the order {} as FAILED.", orderId);

        final Delivery delivery = setDeliveryStateValue(orderId,DeliveryState.FAILED);

        log.debug("Sending request to Order service to mark delivery {} as FAILED.",
                delivery.getDeliveryId());
        orderClient.markDeliveryFailed(orderId);
        log.debug("Delivery is marked in order service as FAILED.");

        deliveryRepository.save(delivery);
    }

    @Override
    @Transactional
    public BigDecimal calculateDeliveryCost(final OrderDto orderDto) {
        log.debug("Calculating delivery cost for the order {}.", orderDto);

        final Delivery delivery = getDeliveryOrThrow(orderDto.getDeliveryId());
        return deliveryCostCalculator.calculate(orderDto,delivery);
    }

    private Delivery getDeliveryOrThrow(final UUID orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Order ID: " + orderId));
    }

    private void validateDeliveryDoesNotExist(final UUID orderId) {
        log.debug("Validating delivery for order ID {} does not exist in the DB.", orderId);
        if (deliveryRepository.existsByOrderId(orderId)) {
            throw new DeliveryForSpecifiedOrderAlreadyExists("Order ID: " + orderId);
        }
    }

    private Delivery setDeliveryStateValue(final UUID orderId, final DeliveryState newState) {

        final Delivery delivery = getDeliveryOrThrow(orderId);
        delivery.setDeliveryState(newState);
        return delivery;
    }
}

