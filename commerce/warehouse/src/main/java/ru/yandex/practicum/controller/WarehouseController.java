package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.service.WarehouseService;
import ru.yandex.practicum.dto.*;

@RestController
@Validated
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/v1/warehouse")
public class WarehouseController {
    private final WarehouseService warehouseService;

    @ResponseStatus(HttpStatus.OK)
    @PutMapping
    public void createProductInWarehouse(@RequestBody NewProductInWarehouse request) {
        log.info("New item adding request{}", request);
        warehouseService.createProductInWarehouse(request);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/check")
    public ReserveProductsDto checkCountProducts(@RequestBody CartDto cartDto) {
        log.info("Item count checking request{}", cartDto);
        return warehouseService.checkCountProducts(cartDto);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/add")
    public void addProductInWarehouse(@RequestBody AddProductInWarehouse request) {
        log.info("Adding item request{}", request);
        warehouseService.addProductInWarehouse(request);
    }

    @ResponseStatus(HttpStatus.OK)
    @GetMapping("/address")
    public AddressWarehouseDto getAddressWarehouse() {
        log.info("Warehouse address request");
        return warehouseService.getAddressWarehouse();
    }
}
