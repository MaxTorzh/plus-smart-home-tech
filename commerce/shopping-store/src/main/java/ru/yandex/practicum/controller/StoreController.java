package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.service.StoreService;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.SetProductCountState;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.util.List;

@RestController
@Validated
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class StoreController {
    private final StoreService storeService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<ProductDto> getProductsByCategory(
            @RequestParam String category,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "10") @Min(1) int size,
            @RequestParam(defaultValue = "productId") String sort) {

        Pageable pageable = PageRequest.of(page, size, Sort.by(sort));
        log.info("Request for getting item list for category {} with pageable {}", category, pageable);
        return storeService.getProductsByCategory(category, pageable);
    }

    @PostMapping
    public ResponseEntity<ProductDto> createProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("New product request {}", productDto);
        ProductDto createdProduct = storeService.createProduct(productDto);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdProduct);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("Update product request {}", productDto);
        return storeService.updateProduct(productDto);
    }

    @PostMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto removeProduct(@RequestParam String productId) {
        log.info("Remove product request {}", productId);
        return storeService.removeProduct(productId);
    }

    @PostMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto changeState(@Valid @RequestBody SetProductCountState request) {
        log.info("Item status request {}", request);
        return storeService.changeState(request);
    }

    @GetMapping("/{productId}")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto getInfoByProduct(@PathVariable String productId) {
        log.info("Item info request {}", productId);
        return storeService.getInfoByProduct(productId);
    }

    @GetMapping("/products")
    @ResponseStatus(HttpStatus.OK)
    public List<ProductDto> getAllProducts(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "10") @Min(1) int size) {

        Pageable pageable = PageRequest.of(page, size);
        log.info("Request for getting all products with pageable {}", pageable);
        return storeService.getAllProducts(pageable);
    }
}