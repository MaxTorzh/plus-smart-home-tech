package ru.yandex.practicum.exception;

public class NoProductsInShoppingCartException extends ApiException {

    public NoProductsInShoppingCartException(final String logDetails) {
        super(ExceptionReason.NO_PRODUCTS_IN_SHOPPING_CART, logDetails);
    }
}
