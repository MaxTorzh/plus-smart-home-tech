package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

/**
 * Client API interface for managing the shopping cart in an online store.
 */
@FeignClient(name = "shopping-cart", path = "/api/v1/shopping-cart")
public interface ShoppingCartOperations {

    /**
     * Retrieves the current shopping cart for an authenticated user. If a cart already exists, it is
     * returned; otherwise, a new one is created.
     *
     * @param username the authenticated user
     * @return the existing or newly created shopping cart
     */
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto getShoppingCart(@RequestParam @NotBlank String username);

    /**
     * Adds products to the shopping cart.
     *
     * @param products a mapping of product IDs to the selected quantity
     * @param username the authenticated user
     * @return the updated shopping cart
     */
    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto addProductToCart(
            @RequestBody @NotEmpty Map<@NotNull UUID, @NotNull @Positive Long> products,
            @RequestParam @NotBlank String username);

    /**
     * Deactivates the current shopping cart for the user.
     *
     * @param username the authenticated user
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.OK)
    void deactivateCurrentCart(@RequestParam @NotBlank String username);

    /**
     * Modifies the contents of the shopping cart by retaining only the specified products. All other
     * products are removed.
     *
     * @param username the authenticated user
     * @param products a set of product IDs to retain in the cart
     * @return the updated shopping cart
     */
    @PutMapping("/remove")
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto removeProductsFromCart(@RequestParam @NotBlank String username,
                                           @RequestBody Set<@NotNull UUID> products);

    /**
     * Changes the quantity of a product in the shopping cart.
     *
     * @param username the authenticated user
     * @param request  the request to update the product quantity
     * @return the updated shopping cart
     */
    @PutMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    ShoppingCartDto changeQuantity(@RequestParam @NotBlank String username,
                                   @RequestBody @Valid ChangeProductQuantityRequest request);
}