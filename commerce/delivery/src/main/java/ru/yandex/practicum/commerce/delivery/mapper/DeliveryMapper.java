package ru.yandex.practicum.commerce.delivery.mapper;

import java.util.Objects;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;

@UtilityClass
public class DeliveryMapper {

    public Delivery toEntity(final DeliveryDto deliveryDto) {
        Objects.requireNonNull(deliveryDto);
        return Delivery.builder()
                .orderId(deliveryDto.getOrderId())
                .deliveryState(deliveryDto.getDeliveryState())
                .fromAddress(AddressMapper.toEntity(deliveryDto.getFromAddress()))
                .toAddress(AddressMapper.toEntity(deliveryDto.getToAddress()))
                .build();
    }

    public static DeliveryDto toDto(final Delivery delivery) {
        Objects.requireNonNull(delivery);
        return DeliveryDto.builder()
                .deliveryId(delivery.getDeliveryId())
                .orderId(delivery.getOrderId())
                .deliveryState(delivery.getDeliveryState())
                .fromAddress(AddressMapper.toDto(delivery.getFromAddress()))
                .toAddress(AddressMapper.toDto(delivery.getToAddress()))
                .build();
    }

    public static ShippedToDeliveryRequest toShipRequest(final UUID orderId, final UUID deliveryId) {
        Objects.requireNonNull(orderId);
        Objects.requireNonNull(deliveryId);
        return ShippedToDeliveryRequest.builder()
                .orderId(orderId)
                .deliveryId(deliveryId)
                .build();
    }
}