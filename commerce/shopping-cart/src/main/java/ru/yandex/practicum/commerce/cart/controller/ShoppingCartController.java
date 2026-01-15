package ru.yandex.practicum.commerce.cart.controller;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.feign.ShoppingCartOperations;
import ru.yandex.practicum.commerce.cart.service.ShoppingCartService;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

/**
 * REST controller for managing shopping cart operations.
 * <p>
 * Implements {@link ShoppingCartOperations}
 */
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
@Slf4j
@Validated
public class ShoppingCartController implements ShoppingCartOperations {

    private final ShoppingCartService shoppingCartService;

    @Override
    public ShoppingCartDto getShoppingCart(final String username) {
        log.info("Received request to view shopping cart by User: {}.", username);
        final ShoppingCartDto cart = shoppingCartService.getShoppingCart(username);
        log.info("Returning shopping cart with ID: {}.", cart.getShoppingCartId());
        return cart;
    }

    @Override
    public ShoppingCartDto addProductToCart(final Map<UUID, Long> products, final String username) {
        log.info("Received request to add products to the cart by User: {}.", username);
        final ShoppingCartDto cartDto = shoppingCartService.addProductsToCart(username, products);
        log.info("Returning updated shopping cart ID {}", cartDto.getShoppingCartId());
        return cartDto;
    }

    @Override
    public void deactivateCurrentCart(final String username) {
        log.info("Received request to deactivate shopping cart for user {}.", username);
        shoppingCartService.deactivateShoppingCart(username);
        log.info("Shopping cart deactivated successfully.");
    }

    @Override
    public ShoppingCartDto removeProductsFromCart(final String username, final Set<UUID> products) {
        log.info("Received request to retain {} products and remove others from the cart of user {}.",
                products.size(), username);
        final ShoppingCartDto cartDto = shoppingCartService.retainProductsInTheCart(username, products);
        log.info("Returning updated shopping cart with ID {}", cartDto.getShoppingCartId());
        return cartDto;
    }

    @Override
    public ShoppingCartDto changeQuantity(final String username,
                                          final ChangeProductQuantityRequest request) {
        log.info("Received request to change quantity of product {} by user {}.",
                request.getProductId(), username);
        final ShoppingCartDto cartDto = shoppingCartService.changeProductQuantity(username, request);
        log.info("Returning shopping cart ID {} with updated product.", cartDto.getShoppingCartId());
        return cartDto;
    }
}