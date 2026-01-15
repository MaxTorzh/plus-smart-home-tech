package ru.yandex.practicum.commerce.cart.mapper;

import java.util.Objects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

/**
 * Utility class for mapping between ShoppingCart and ShoppingCartDto objects.
 */
@UtilityClass
@Slf4j
public class ShoppingCartMapper {

    public ShoppingCartDto toDto(final ShoppingCart cart) {
        log.debug("Mapping ShoppingCart {} to ShoppingCartDto.", cart);
        Objects.requireNonNull(cart);
        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getCartId())
                .products(cart.getProducts())
                .build();
    }
}
