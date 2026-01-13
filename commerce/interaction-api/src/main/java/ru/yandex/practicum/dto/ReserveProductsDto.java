package ru.yandex.practicum.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReserveProductsDto {
    @NotBlank
    private double deliveryWeight;
    @NotBlank
    private double deliveryVolume;
    @NotBlank
    private boolean fragile;
}
