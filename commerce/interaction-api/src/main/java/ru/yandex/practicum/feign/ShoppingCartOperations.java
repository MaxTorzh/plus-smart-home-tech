package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@FeignClient(name = "shopping-cart", path = "/api/v1/shopping-cart")
public interface ShoppingCartOperations {

    @GetMapping
    ShoppingCartDto getShoppingCart(@RequestParam @NotBlank String username);

    @PutMapping
    ShoppingCartDto addProductToCart(
            @RequestBody @NotEmpty Map<@NotNull UUID, @NotNull @Positive Long> products,
            @RequestParam @NotBlank String username);

    @DeleteMapping
    void deactivateCurrentCart(@RequestParam @NotBlank String username);

    @PostMapping("/remove")
    ShoppingCartDto removeProductsFromCart(
            @RequestParam @NotBlank String username,
            @RequestBody Set<@NotNull UUID> products);

    @PostMapping("/change-quantity")
    ShoppingCartDto changeQuantity(
            @RequestParam @NotBlank String username,
            @RequestBody @Valid ChangeProductQuantityRequest request);
}