package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.dto.ProductDto;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ProductMapper {

    default Product productDtoToProduct(ProductDto productDto) {
        return null;
    }

    ProductDto productToProductDto(Product product);

    List<ProductDto> mapListProducts(List<Product> products);
}
