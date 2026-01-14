package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.service.CartService;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.ChangeProductCount;
import ru.yandex.practicum.dto.ReserveProductsDto;
import java.util.Map;

@RestController
@Validated
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class CartController {
    private final CartService cartService;

    @ResponseStatus(HttpStatus.OK)
    @GetMapping
    public CartDto getShoppingCart(@RequestParam String username) {
        log.info("Request for getting the cart from user{}", username);
        return cartService.getShoppingCart(username);
    }

    @ResponseStatus(HttpStatus.OK)
    @PutMapping
    public CartDto addProductsToCart(@RequestParam String username,
                                     @RequestBody Map<String, Long> items) {
        log.info("Request for items adding{} in user cart{}", items, username);

        return cartService.addProductsToCart(username, items);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping
    public void deleteUserCart(@RequestParam String username) {
        log.info("Request for cart deactivation for user{}", username);
        cartService.deleteUserCart(username);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/remove")
    public CartDto changeCart(@RequestParam String username,
                              @RequestBody Map<String, Long> items) {
        log.info("Request for cart changing{} for cart user{}",  items, username);
        return cartService.changeCart(username, items);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/change-quantity")
    public CartDto changeCountProductsOfCart(@RequestParam String username,
                                             @RequestBody ChangeProductCount request) {
        log.info("Request for item amount changing{} for cart user{}", request, username);
        return cartService.changeCountProductInCart(username, request);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("/booking")
    public ReserveProductsDto reserveProducts(@RequestParam String nameUser) {
        log.info("Request for warehouse items reserving from user{}", nameUser);
        return cartService.reserveProducts(nameUser);
    }
}
