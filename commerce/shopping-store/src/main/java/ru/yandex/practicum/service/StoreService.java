package ru.yandex.practicum.service;

import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.SetProductCountState;

import java.util.List;

public interface StoreService {

    List<ProductDto> getProductsByCategory(String category, Pageable pageable);

    List<ProductDto> getAllProducts(Pageable pageable);

    ProductDto createProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    ProductDto removeProduct(String productId);

    ProductDto changeState(SetProductCountState request);

    ProductDto getInfoByProduct(String productId);
}