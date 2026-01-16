package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;

import java.util.UUID;

@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface ShoppingStoreOperations {

    @GetMapping
    Page<ProductDto> getProductsByCategory(
            @RequestParam("category") ProductCategory category,
            Pageable pageable);

    @GetMapping("/{productId}")
    ProductDto getProductById(@PathVariable("productId") UUID productId);

    @GetMapping("/products")
    Page<ProductDto> getAllProducts(Pageable pageable);

    @PostMapping
    ProductDto createProduct(@Valid @RequestBody ProductDto productDto);

    @PutMapping
    ProductDto updateProduct(@Valid @RequestBody ProductDto productDto);

    @PostMapping("/removeProductFromStore")
    boolean removeProduct(@RequestBody UUID productId);

    @PostMapping("/quantityState")
    ProductDto updateQuantityState(@Valid @RequestBody SetProductQuantityStateRequest request);
}