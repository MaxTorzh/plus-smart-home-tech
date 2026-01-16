package ru.yandex.practicum.commerce.cart.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.cart.service.ShoppingCartService;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
@Slf4j
@Validated
public class ShoppingCartController {

    private final ShoppingCartService shoppingCartService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public ShoppingCartDto getShoppingCart(
            @Valid @RequestParam @NotBlank String username) {
        log.info("GET shopping cart for user: {}", username);
        return shoppingCartService.getShoppingCart(username);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public ShoppingCartDto addProductToCart(
            @Valid @NotEmpty @RequestBody Map<@NotNull UUID, @NotNull @Positive Long> products,
            @Valid @RequestParam @NotBlank String username) {
        log.info("PUT add products to cart for user: {}, products: {}", username, products);
        return shoppingCartService.addProductsToCart(username, products);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCurrentCart(
            @Valid @RequestParam @NotBlank String username) {
        log.info("DELETE deactivate cart for user: {}", username);
        shoppingCartService.deactivateShoppingCart(username);
    }

    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.OK)
    public ShoppingCartDto removeProductsFromCart(
            @Valid @RequestParam @NotBlank String username,
            @Valid @NotEmpty @RequestBody Set<@NotNull UUID> products) {
        log.info("POST remove products from cart for user: {}, products: {}", username, products);
        return shoppingCartService.retainProductsInTheCart(username, products);
    }

    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public ShoppingCartDto changeQuantity(
            @Valid @RequestParam @NotBlank String username,
            @Valid @RequestBody ChangeProductQuantityRequest request) {
        log.info("POST change quantity for user: {}, request: {}", username, request);
        return shoppingCartService.changeProductQuantity(username, request);
    }
}