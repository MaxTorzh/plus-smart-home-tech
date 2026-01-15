package ru.yandex.practicum.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.types.ProductCategory;
import ru.yandex.practicum.types.ProductState;
import ru.yandex.practicum.types.QuantityState;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {

    private String productId;

    @NotBlank(message = "Product name cannot be blank")
    private String productName;

    @NotBlank(message = "Description cannot be blank")
    private String description;

    private String imageSrc;

    @NotNull(message = "Quantity state cannot be null")
    private QuantityState quantityState;

    @NotNull(message = "Product state cannot be null")
    private ProductState productState;

    private Integer rating;

    private ProductCategory productCategory;

    @NotNull(message = "Price cannot be null")
    @Min(value = 0, message = "Price must be greater than or equal to 0")
    private Float price;
}
