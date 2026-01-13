package ru.yandex.practicum.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.types.QuantityState;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SetProductCountState {
    @NotBlank
    private String productId;

    @NotBlank
    private QuantityState quantityState;
}
