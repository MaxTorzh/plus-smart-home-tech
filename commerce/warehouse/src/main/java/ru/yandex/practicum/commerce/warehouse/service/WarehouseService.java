package ru.yandex.practicum.commerce.warehouse.service;

import java.util.Map;
import java.util.UUID;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;

/**
 * Business logic interface for warehouse operations.
 */
public interface WarehouseService {

    void addNewProduct(NewProductInWarehouseRequest product);

    void increaseProductQuantity(AddProductToWarehouseRequest request);

    BookedProductsDto checkStock(ShoppingCartDto shoppingCart);

    AddressDto getAddress();

    void sendToDelivery(ShippedToDeliveryRequest request);

    BookedProductsDto bookProducts(AssemblyProductsForOrderRequest request);

    void returnProducts(Map<UUID, Long> products);
}
