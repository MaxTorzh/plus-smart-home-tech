package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.dto.ProductDto;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ProductMapper {

    public Product toProduct(ProductDto dto) {
        if (dto == null) {
            return null;
        }

        Product product = new Product();
        product.setProductId(dto.getProductId());
        product.setProductName(dto.getProductName());
        product.setProductCategory(dto.getProductCategory());
        product.setPrice(dto.getPrice());
        product.setQuantityState(dto.getQuantityState());
        product.setProductState(dto.getProductState());
        return product;
    }

    public ProductDto toProductDto(Product product) {
        if (product == null) {
            return null;
        }

        ProductDto dto = new ProductDto();
        dto.setProductId(product.getProductId());
        dto.setProductName(product.getProductName());
        dto.setProductCategory(product.getProductCategory());
        dto.setPrice(product.getPrice());
        dto.setQuantityState(product.getQuantityState());
        dto.setProductState(product.getProductState());
        return dto;
    }

    public List<ProductDto> toProductDtoList(List<Product> products) {
        if (products == null) {
            return List.of();
        }

        return products.stream()
                .map(this::toProductDto)
                .collect(Collectors.toList());
    }
}