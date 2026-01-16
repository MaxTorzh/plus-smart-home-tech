package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
@Slf4j
@Validated
public class WarehouseController {

    private final WarehouseService warehouseService;

    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void addProduct(@Valid @RequestBody NewProductInWarehouseRequest product) {
        log.info("PUT add product to warehouse: {}", product);
        warehouseService.addNewProduct(product);
    }

    @PutMapping("/add")
    @ResponseStatus(HttpStatus.OK)
    public void increaseProductQuantity(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("PUT increase product quantity: {}", request);
        warehouseService.increaseProductQuantity(request);
    }

    @PutMapping("/check")
    @ResponseStatus(HttpStatus.OK)
    public BookedProductsDto checkStock(@Valid @RequestBody ShoppingCartDto shoppingCart) {
        log.info("PUT check stock for cart: {}", shoppingCart.getShoppingCartId());
        return warehouseService.checkStock(shoppingCart);
    }

    @GetMapping("/address")
    @ResponseStatus(HttpStatus.OK)
    public AddressDto getWarehouseAddress() {
        log.info("GET warehouse address");
        return warehouseService.getAddress();
    }

    @PutMapping("/shipped")
    @ResponseStatus(HttpStatus.OK)
    public void sendToDelivery(@Valid @RequestBody ShippedToDeliveryRequest request) {
        log.info("PUT send to delivery: {}", request);
        warehouseService.sendToDelivery(request);
    }

    @PutMapping("/return")
    @ResponseStatus(HttpStatus.OK)
    public void acceptReturn(@RequestBody Map<UUID, Long> products) {
        log.info("PUT accept return: {}", products);
        warehouseService.returnProducts(products);
    }

    @PutMapping("/assembly")
    @ResponseStatus(HttpStatus.OK)
    public BookedProductsDto assembleProductsForOrder(
            @Valid @RequestBody AssemblyProductsForOrderRequest request) {
        log.info("PUT assemble products for order: {}", request.getOrderId());
        return warehouseService.bookProducts(request);  // НЕ возвращать null!
    }
}