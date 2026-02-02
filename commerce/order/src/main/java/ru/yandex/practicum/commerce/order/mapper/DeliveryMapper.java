package ru.yandex.practicum.commerce.order.mapper;

import java.util.UUID;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.DeliveryState;
import ru.yandex.practicum.dto.warehouse.AddressDto;

/**
 * Utility class for managing mapping to {@link DeliveryDto} objects.
 */
@UtilityClass
@Slf4j
public class DeliveryMapper {

    public DeliveryDto mapToDeliveryDto(final UUID orderId,
                                        final AddressDto deliveryAddress,
                                        final AddressDto warehouseAddress) {
        log.debug("Building delivery request from : ID {}, from {}, to {}.",
                orderId, warehouseAddress, deliveryAddress);
        return DeliveryDto.builder()
                .orderId(orderId)
                .fromAddress(warehouseAddress)
                .toAddress(deliveryAddress)
                .deliveryState(DeliveryState.CREATED)
                .build();
    }

}

