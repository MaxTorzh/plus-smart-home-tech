package ru.yandex.practicum.commerce.store.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.*;
import ru.yandex.practicum.commerce.store.service.ShoppingStoreService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@Slf4j
public class ShoppingStoreController {

    private final ShoppingStoreService shoppingStoreService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public Page<ProductDto> getProductsByCategory(
            @RequestParam ProductCategory category,
            Pageable pageable) {
        log.info("GET products by category: {}", category);
        return shoppingStoreService.getProductsByCategory(category, pageable);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        log.info("PUT create product: {}", productDto);
        return shoppingStoreService.addProduct(productDto);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateProduct(@RequestBody ProductDto productDto) {
        log.info("POST update product: {}", productDto);
        return shoppingStoreService.updateProduct(productDto);
    }

    @PostMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public boolean removeProduct(@RequestBody UUID productId) {
        log.info("POST remove product: {}", productId);
        return shoppingStoreService.removeProduct(productId);
    }

    @PostMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    public boolean updateQuantityState(@RequestBody SetProductQuantityStateRequest request) {
        log.info("POST update quantity state: {}", request);
        return shoppingStoreService.updateQuantityState(request);
    }

    @GetMapping("/{productId}")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto getProductById(@PathVariable UUID productId) {
        log.info("GET product by id: {}", productId);
        return shoppingStoreService.getProductById(productId);
    }

    @GetMapping("/products")
    @ResponseStatus(HttpStatus.OK)
    public Page<ProductDto> getAllProducts(Pageable pageable) {
        log.info("GET all products");
        return shoppingStoreService.getAllProducts(pageable);
    }
}