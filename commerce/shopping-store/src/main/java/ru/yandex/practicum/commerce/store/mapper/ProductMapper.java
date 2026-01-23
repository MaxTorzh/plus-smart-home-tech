package ru.yandex.practicum.commerce.store.mapper;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.commerce.store.model.Product;

import java.math.BigDecimal;
import java.util.Objects;

@UtilityClass
@Slf4j
public class ProductMapper {

    public Product toEntity(ProductDto productDto) {
        Objects.requireNonNull(productDto, "ProductDto cannot be null");

        return Product.builder()
                .productId(productDto.getProductId())
                .productName(productDto.getProductName())
                .description(productDto.getDescription())
                .imageSrc(productDto.getImageSrc())
                .quantityState(productDto.getQuantityState())
                .productState(productDto.getProductState())
                .productCategory(productDto.getProductCategory())
                .price(productDto.getPrice())
                .rating(productDto.getRating() != null ?
                        productDto.getRating().setScale(1) : BigDecimal.ZERO.setScale(1))
                .build();
    }

    public ProductDto toDto(Product product) {
        Objects.requireNonNull(product, "Product cannot be null");

        return ProductDto.builder()
                .productId(product.getProductId())
                .productName(product.getProductName())
                .description(product.getDescription())
                .imageSrc(product.getImageSrc())
                .quantityState(product.getQuantityState())
                .productState(product.getProductState())
                .productCategory(product.getProductCategory())
                .price(product.getPrice())
                .rating(product.getRating())
                .build();
    }
}