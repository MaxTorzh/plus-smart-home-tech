package ru.yandex.practicum.commerce.cart.service;

import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Service interface for managing products in the shopping cart.
 */
public interface ShoppingCartService {

    ShoppingCartDto getShoppingCart(String username);

    ShoppingCartDto addProductsToCart(String username, Map<UUID, Long> products);

    void deactivateShoppingCart(String username);

    ShoppingCartDto retainProductsInTheCart(String username, Set<UUID> products);

    ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request);
}
