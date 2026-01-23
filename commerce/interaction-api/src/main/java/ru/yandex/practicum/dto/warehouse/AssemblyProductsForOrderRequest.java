package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO class representing request to assemble listed products for the given order.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssemblyProductsForOrderRequest {

    @NotNull
    private UUID orderId;

    @NotNull
    @NotEmpty
    private Map<UUID, Long> products;
}
