package ru.yandex.practicum.dto.cart;

import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a shopping cart in the online store.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ShoppingCartDto {

    /**
     * The unique identifier of the shopping cart in the database.
     */
    @NotNull
    private UUID shoppingCartId;

    /**
     * A mapping of product IDs to their selected quantity.
     */
    @NotNull
    private Map<UUID, Long> products;
}
