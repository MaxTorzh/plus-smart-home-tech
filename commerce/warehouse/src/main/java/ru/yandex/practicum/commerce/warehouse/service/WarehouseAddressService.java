package ru.yandex.practicum.commerce.warehouse.service;

import ru.yandex.practicum.dto.warehouse.AddressDto;

/**
 * Service interface for retrieving the warehouse address.
 */
public interface WarehouseAddressService {

    AddressDto getAddress();
}
