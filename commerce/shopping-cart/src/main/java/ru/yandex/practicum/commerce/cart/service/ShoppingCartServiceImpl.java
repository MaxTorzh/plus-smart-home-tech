package ru.yandex.practicum.commerce.cart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.exception.ShoppingCartModificationException;
import ru.yandex.practicum.feign.WarehouseOperations;
import ru.yandex.practicum.commerce.cart.mapper.ShoppingCartMapper;
import ru.yandex.practicum.commerce.cart.model.ShoppingCart;
import ru.yandex.practicum.commerce.cart.model.ShoppingCartState;
import ru.yandex.practicum.commerce.cart.repository.ShoppingCartRepository;
import ru.yandex.practicum.commerce.cart.utility.UuidGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Service implementation for managing shopping cart operations.
 * <p>
 * This service allows users to interact with their shopping cart, including adding products,
 * changing quantities, removing products, and deactivating the cart. It ensures that operations are
 * performed only on active carts, while maintaining proper validation for user input and cart
 * state.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShoppingCartServiceImpl implements ShoppingCartService {

    private final ShoppingCartRepository cartRepository;
    private final WarehouseOperations warehouseClient;
    private final UuidGenerator uuidGenerator;

    @Transactional
    @Override
    public ShoppingCartDto getShoppingCart(final String username) {
        validateUser(username);
        log.debug("Retrieving shopping cart for the user {}.", username);

        final ShoppingCart shoppingCart = getOrCreateShoppingCart(username);
        return ShoppingCartMapper.toDto(shoppingCart);
    }

    @Transactional
    @Override
    public ShoppingCartDto addProductsToCart(final String username, final Map<UUID, Long> products) {
        validateUser(username);
        log.debug("Adding products {} to the shopping cart for the user {}.", products, username);

        final ShoppingCart shoppingCart = getOrCreateShoppingCart(username);
        validateCartIsActive(shoppingCart);

        products.forEach((key, value) -> shoppingCart.getProducts().merge(key, value, Long::sum));

        log.debug("Shopping cart after adding products: {}", shoppingCart);

        final ShoppingCartDto cartDto = ShoppingCartMapper.toDto(shoppingCart);
        validateAllProductsAvailable(cartDto);
        cartRepository.save(shoppingCart);
        return cartDto;
    }

    @Transactional
    @Override
    public void deactivateShoppingCart(final String username) {
        validateUser(username);
        log.debug("Deactivating Shopping Cart for user {}.", username);

        final ShoppingCart shoppingCart = getOrCreateShoppingCart(username);
        if (shoppingCart.getCartState() == ShoppingCartState.DEACTIVATED) {
            log.debug("Cart {} is already deactivated.", shoppingCart.getCartId());
            return;
        }
        shoppingCart.setCartState(ShoppingCartState.DEACTIVATED);
        cartRepository.save(shoppingCart);
        log.debug("Shop[ing cart with ID {} was successfully deactivated for username {}.",
                shoppingCart.getCartId(), shoppingCart.getUsername());
    }

    @Transactional
    @Override
    public ShoppingCartDto retainProductsInTheCart(final String username, final Set<UUID> products) {
        validateUser(username);
        log.debug("Removing products with ID: {} from the shopping cart of user {}.", products,
                username);

        final ShoppingCart shoppingCart = getOrCreateShoppingCart(username);
        validateCartIsActive(shoppingCart);
        validateProductsInTheCart(shoppingCart, products);

        shoppingCart.getProducts().keySet().retainAll(products);

        final ShoppingCart updatedCart = cartRepository.save(shoppingCart);
        log.debug("Updated cart: {}.", updatedCart);
        return ShoppingCartMapper.toDto(updatedCart);
    }

    @Transactional
    @Override
    public ShoppingCartDto changeProductQuantity(final String username,
                                                 final ChangeProductQuantityRequest request) {
        log.debug("Changing quantity of the product {} to {} by user {}.",
                request.getProductId(), request.getNewQuantity(), username);
        validateUser(username);

        final ShoppingCart shoppingCart = getOrCreateShoppingCart(username);
        validateCartIsActive(shoppingCart);

        if (!shoppingCart.getProducts().containsKey(request.getProductId())) {
            throw new NoProductsInShoppingCartException(
                    "No such Product in the cart - " + request.getProductId());
        }

        shoppingCart.getProducts().put(request.getProductId(), request.getNewQuantity());

        final ShoppingCart updatedCart = cartRepository.save(shoppingCart);
        log.debug("Updated quantity for the product {} in the cart: {} .",
                request.getProductId(), updatedCart);
        return ShoppingCartMapper.toDto(updatedCart);
    }

    private void validateAllProductsAvailable(final ShoppingCartDto cartDto) {
        log.debug("Sending request to the warehouse to check product availability: {}", cartDto);
        warehouseClient.checkStock(cartDto);
    }

    private void validateUser(final String username) {
        log.debug("Validating username {}.", username);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Validation username " + username + " failed.");
        }
    }

    private ShoppingCart getOrCreateShoppingCart(final String username) {
        log.debug("Retrieving shopping cart if exist from DB for username {}.", username);
        return cartRepository.findByUsername(username)
                .orElseGet(() -> createShoppingCart(username));
    }

    private ShoppingCart createShoppingCart(final String username) {
        log.debug("Creating new ACTIVE cart for User {}.", username);
        final ShoppingCart shoppingCart = ShoppingCart.builder()
                .cartId(uuidGenerator.generate())
                .username(username)
                .cartState(ShoppingCartState.ACTIVE)
                .products(new HashMap<>())
                .build();
        final ShoppingCart createdCart = cartRepository.save(shoppingCart);
        log.debug("Created new cart with ID {} for user {}", createdCart.getCartId(),
                createdCart.getUsername());
        return createdCart;
    }

    private void validateCartIsActive(final ShoppingCart shoppingCart) {
        log.debug("Validating shopping cart is allowed to modify, {}.", shoppingCart);
        if (ShoppingCartState.DEACTIVATED.equals(shoppingCart.getCartState())) {
            throw new ShoppingCartModificationException(
                    "Current state is " + shoppingCart.getCartState());
        }
    }

    private void validateProductsInTheCart(final ShoppingCart shoppingCart,
                                           final Set<UUID> products) {
        log.debug("Ensuring shopping cart {} contains products {}.", shoppingCart, products);

        if (shoppingCart.getProducts().isEmpty() ||
                !shoppingCart.getProducts().keySet().containsAll(products)) {
            throw new NoProductsInShoppingCartException(
                    "Shopping cart content: " + shoppingCart.getProducts());
        }
    }
}
