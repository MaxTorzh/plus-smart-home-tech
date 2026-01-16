package ru.yandex.practicum.exception;

public class ShoppingCartModificationException extends ApiException {

    public ShoppingCartModificationException(final String logDetails) {
        super(ExceptionReason.SHOPPING_CART_MODIFICATION_NOT_ALLOWED, logDetails);
    }
}
