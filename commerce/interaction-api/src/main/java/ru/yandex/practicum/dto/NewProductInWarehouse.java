package ru.yandex.practicum.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NewProductInWarehouse {
    @NotBlank
    private String productId;

    private boolean fragile;

    @NotBlank
    private SizeDto dimension;

    @NotBlank
    @Min(value = 1, message = "Weight should not be less than 1")
    private double weight;
}
