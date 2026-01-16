package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse", path = "/api/v1/warehouse")
public interface WarehouseOperations {

    @PutMapping
    void addProduct(@Valid @RequestBody NewProductInWarehouseRequest product);

    @PutMapping("/check")
    BookedProductsDto checkStock(@Valid @RequestBody ShoppingCartDto shoppingCart);

    @PutMapping("/add")
    void increaseProductQuantity(@Valid @RequestBody AddProductToWarehouseRequest request);

    @GetMapping("/address")
    AddressDto getWarehouseAddress();

    @PutMapping("/shipped")
    void sendToDelivery(@Valid @RequestBody ShippedToDeliveryRequest request);

    @PutMapping("/return")
    void acceptReturn(@RequestBody Map<UUID, Long> products);

    @PutMapping("/assembly")
    BookedProductsDto assembleProductsForOrder(@Valid @RequestBody AssemblyProductsForOrderRequest request);
}