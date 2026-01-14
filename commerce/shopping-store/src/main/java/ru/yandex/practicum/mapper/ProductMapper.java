package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.dto.ProductDto;

import java.util.List;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface ProductMapper {

    @Mapping(target = "id", ignore = true) // если id автоинкрементный
    @Mapping(source = "productId", target = "productId")
    @Mapping(source = "productName", target = "productName")
    @Mapping(source = "productCategory", target = "productCategory")
    @Mapping(source = "price", target = "price")
    @Mapping(source = "quantityState", target = "quantityState")
    @Mapping(source = "productState", target = "productState")
    Product productDtoToProduct(ProductDto productDto);

    @Mapping(source = "productId", target = "productId")
    @Mapping(source = "productName", target = "productName")
    @Mapping(source = "productCategory", target = "productCategory")
    @Mapping(source = "price", target = "price")
    @Mapping(source = "quantityState", target = "quantityState")
    @Mapping(source = "productState", target = "productState")
    ProductDto productToProductDto(Product product);

    List<ProductDto> mapListProducts(List<Product> products);
}
