package ru.yandex.practicum.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.types.QuantityState;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SetProductQuantityStateRequest {

    @NotBlank(message = "Product ID cannot be blank")
    private String productId;

    @NotNull(message = "Quantity state cannot be null")
    private QuantityState quantityState;
}
