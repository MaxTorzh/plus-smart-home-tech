package ru.yandex.practicum.dto.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;

/**
 * Request to create a new order from the shopping cart.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateNewOrderRequest {

    @NotNull
    @Valid
    private ShoppingCartDto shoppingCart;

    @NotNull
    @Valid
    private AddressDto deliveryAddress;

}
