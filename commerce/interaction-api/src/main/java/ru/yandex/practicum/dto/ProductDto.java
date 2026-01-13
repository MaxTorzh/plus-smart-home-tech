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
    @Min(value = 1, message = "Рейтинг не может быть меньше 1")
    @Max(value = 5, message = "Рейтинг не может быть больше 5")
    private int rating;
    private ProductCategory productCategory;
    @NotBlank
    @Min(value = 1, message = "Стоимость не может быть меньше 1")
    private float price;
}
