package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.FORBIDDEN)
public class ShoppingCartModificationException extends ApiException {

    public ShoppingCartModificationException(final String logDetails) {
        super(ExceptionReason.SHOPPING_CART_MODIFICATION_NOT_ALLOWED, logDetails);
    }
}
