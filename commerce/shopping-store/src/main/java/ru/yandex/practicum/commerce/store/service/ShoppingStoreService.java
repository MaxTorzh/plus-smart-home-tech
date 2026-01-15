package ru.yandex.practicum.commerce.store.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;

import java.util.UUID;

/**
 * Service interface for managing products in the shopping store.
 */
public interface ShoppingStoreService {

    ProductDto addProduct(ProductDto productDto);

    Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable);

    ProductDto getProductById(UUID productId);

    ProductDto updateProduct(ProductDto productDto);

    boolean updateQuantityState(SetProductQuantityStateRequest request);

    boolean removeProduct(UUID productId);
}