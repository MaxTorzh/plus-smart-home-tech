package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class NoProductsInShoppingCartException extends ApiException {

    public NoProductsInShoppingCartException(final String logDetails) {
        super(ExceptionReason.NO_PRODUCTS_IN_SHOPPING_CART, logDetails);
    }
}
