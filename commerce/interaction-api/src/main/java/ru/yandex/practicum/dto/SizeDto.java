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
    @Min(value = 1, message = "Width should not be less than 1")
    private double width;
    @NotBlank
    @Min(value = 1, message = "Height should not be less than 1")
    private double height;
    @NotBlank
    @Min(value = 1, message = "Depth should not be less than 1")
    private double depth;
}
