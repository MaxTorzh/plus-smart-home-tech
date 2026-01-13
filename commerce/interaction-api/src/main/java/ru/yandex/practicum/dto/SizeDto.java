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
public class SizeDto {
    @NotBlank
    @Min(value = 1, message = "Ширина не должна быть меньше 1")
    private double width;
    @NotBlank
    @Min(value = 1, message = "Высота не должна быть меньше 1")
    private double height;
    @NotBlank
    @Min(value = 1, message = "Глубина не должна быть меньше 1")
    private double depth;
}
