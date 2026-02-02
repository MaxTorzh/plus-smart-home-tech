package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to return specified product of a given order.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductReturnRequest {

    private UUID orderId;

    @NotNull
    private Map<UUID, Long> products;

}
