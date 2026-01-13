package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.ChangeProductCount;
import ru.yandex.practicum.dto.ReserveProductsDto;
import ru.yandex.practicum.dto.CartDto;

import java.util.Map;

public interface CartService {
    CartDto getShoppingCart(String username);

    CartDto addProductsToCart(String username, Map<String, Long> items);

    void deleteUserCart(String username);

    CartDto changeCart(String username, Map<String, Long> items);

    CartDto changeCountProductInCart(String username, ChangeProductCount request);

    ReserveProductsDto reserveProducts(String nameUser);
}
