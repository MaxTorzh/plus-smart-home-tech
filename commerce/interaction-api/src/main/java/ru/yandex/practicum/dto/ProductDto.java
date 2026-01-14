package ru.yandex.practicum.dto;


import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.types.ProductCategory;
import ru.yandex.practicum.types.ProductState;
import ru.yandex.practicum.types.QuantityState;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductDto {
    @NotBlank
    private String productId;
    @NotBlank
    private String productName;
    @NotBlank
    private String description;
    private String imageSrc;
    @NotBlank
    private QuantityState quantityState;
    @NotBlank
    private ProductState productState;
    @NotBlank
    @Min(value = 1, message = "Rating should not be less than 1")
    @Max(value = 5, message = "Rating should not be more than 5")
    private int rating;
    private ProductCategory productCategory;
    @NotBlank
    @Min(value = 1, message = "Price should not be less than 1")
    private float price;
}
