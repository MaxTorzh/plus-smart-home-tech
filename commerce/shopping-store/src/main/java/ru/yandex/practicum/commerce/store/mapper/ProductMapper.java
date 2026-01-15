package ru.yandex.practicum.commerce.store.mapper;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.commerce.store.model.Product;

import java.util.Objects;

/**
 * Interface for mapping between Product and ProductDto objects.
 */
@UtilityClass
@Slf4j
public class ProductMapper {

    public Product toEntity(final ProductDto productDto) {
        log.debug("Mapping ProductDto {} to Product.", productDto);
        Objects.requireNonNull(productDto);
        return Product.builder()
                .productId(productDto.getProductId())
                .productName(productDto.getProductName())
                .description(productDto.getDescription())
                .imageSrc(productDto.getImageSrc())
                .quantityState(productDto.getQuantityState())
                .productState(productDto.getProductState())
                .productCategory(productDto.getProductCategory())
                .price(productDto.getPrice())
                .build();
    }

    public ProductDto toDto(final Product product) {
        log.debug("Mapping Product {} to ProductDto.", product);
        Objects.requireNonNull(product);
        return ProductDto.builder()
                .productId(product.getProductId())
                .productName(product.getProductName())
                .description(product.getDescription())
                .imageSrc(product.getImageSrc())
                .quantityState(product.getQuantityState())
                .productState(product.getProductState())
                .productCategory(product.getProductCategory())
                .price(product.getPrice())
                .build();
    }
}