package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.mapper.CartMapper;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.repository.CartRepository;
import ru.yandex.practicum.dto.ChangeProductCount;
import ru.yandex.practicum.dto.ReserveProductsDto;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.exception.ConditionsNotMetException;
import ru.yandex.practicum.warehouse.WarehouseClient;

import java.util.Map;

@Service
@Slf4j
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class CartServiceImpl implements CartService {
    private final CartRepository cartRepository;
    private final CartMapper cartMapper;
    private final WarehouseClient warehouseClient;

    @Override
    public CartDto getShoppingCart(String username) {
        checkUserPresence(username);
        return cartMapper.toShoppingCartDto(getCart(username));
    }

    @Transactional
    @Override
    public CartDto addProductsToCart(String username, Map<String, Long> items) {
        checkUserPresence(username);
        ShoppingCart shoppingCart = getCart(username);
        if (shoppingCart == null) {
            shoppingCart = ShoppingCart.builder()
                    .username(username)
                    .products(items)
                    .cartState(true)
                    .build();
        } else {
            Map<String, Long> products = shoppingCart.getProducts();
            products.putAll(items);
        }
        return cartMapper.toShoppingCartDto(cartRepository.save(shoppingCart));
    }

    @Transactional
    @Override
    public void deleteUserCart(String username) {
        checkUserPresence(username);
        ShoppingCart shoppingCart = getCart(username);
        shoppingCart.setCartState(false);
        cartRepository.save(shoppingCart);
    }

    @Transactional
    @Override
    public CartDto changeCart(String username, Map<String, Long> items) {
        checkUserPresence(username);
        ShoppingCart shoppingCart = getCart(username);
        if (shoppingCart == null)
            throw new ConditionsNotMetException("There is no cart for user " + username);
        shoppingCart.setProducts(items);
        return cartMapper.toShoppingCartDto(cartRepository.save(shoppingCart));
    }

    @Transactional
    @Override
    public CartDto changeCountProductInCart(String username, ChangeProductCount request) {
        checkUserPresence(username);
        ShoppingCart shoppingCart = getCart(username);
        if (shoppingCart == null || !shoppingCart.getProducts().containsKey(request.getProductId()))
            throw new ConditionsNotMetException("There is no cart for user " + username);
        shoppingCart.getProducts().put(request.getProductId(), request.getNewQuantity());
        return cartMapper.toShoppingCartDto(cartRepository.save(shoppingCart));
    }

    @Override
    public ReserveProductsDto reserveProducts(String nameUser) {
        checkUserPresence(nameUser);
        ShoppingCart shoppingCart = getCart(nameUser);
        return warehouseClient.checkAvailableProducts(cartMapper.toShoppingCartDto(shoppingCart));
    }

    private void checkUserPresence(String username) {
        if (username == null || username.isEmpty())
            throw new ConditionsNotMetException("There is no user");
    }

    private ShoppingCart getCart(String username) {
        return cartRepository.findByUsername(username);
    }
}
