package ru.yandex.practicum.commerce.store.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.*;
import ru.yandex.practicum.commerce.store.service.ShoppingStoreService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@Slf4j
@Validated
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
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("PUT create product: {}", productDto);
        return shoppingStoreService.addProduct(productDto);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST update product: {}", productDto);
        return shoppingStoreService.updateProduct(productDto);
    }

    @PostMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public boolean removeProduct(@Valid @RequestBody UUID productId) {
        log.info("POST remove product: {}", productId);
        return shoppingStoreService.removeProduct(productId);
    }

    @PostMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    public boolean updateQuantityState(
            @RequestParam(required = false) UUID productId,
            @RequestParam(required = false) String quantityState,
            @RequestBody(required = false) SetProductQuantityStateRequest requestBody) {

        log.info("DEBUG - productId param: {}, quantityState param: {}, requestBody: {}",
                productId, quantityState, requestBody);

        UUID actualProductId;
        QuantityState actualQuantityState;

        if (productId != null && quantityState != null) {
            actualProductId = productId;
            try {
                actualQuantityState = QuantityState.valueOf(quantityState.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid quantity state: " + quantityState);
            }
            log.info("Using parameters from query string");
        }
        else if (requestBody != null && requestBody.getProductId() != null && requestBody.getQuantityState() != null) {
            actualProductId = requestBody.getProductId();
            actualQuantityState = requestBody.getQuantityState();
            log.info("Using parameters from request body");
        }
        else {
            throw new IllegalArgumentException(
                    "Provide either productId and quantityState as query parameters, " +
                            "or provide SetProductQuantityStateRequest in request body");
        }

        SetProductQuantityStateRequest request =
                new SetProductQuantityStateRequest(actualProductId, actualQuantityState);

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