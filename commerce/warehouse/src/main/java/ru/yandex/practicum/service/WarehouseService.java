package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.*;

public interface WarehouseService {
    void createProductInWarehouse(NewProductInWarehouse request);

    ReserveProductsDto checkCountProducts(CartDto cartDto);

    void addProductInWarehouse(AddProductInWarehouse request);

    AddressWarehouseDto getAddressWarehouse();
}
