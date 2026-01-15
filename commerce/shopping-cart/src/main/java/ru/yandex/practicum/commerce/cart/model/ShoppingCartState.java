package ru.yandex.practicum.commerce.cart.model;

/**
 * The ShoppingCartState enum represents the state of a shopping cart, indicating whether it is
 * active (can be modified) or deactivated (frozen and no further modifications are allowed).
 */
public enum ShoppingCartState {
    ACTIVE,
    DEACTIVATED
}