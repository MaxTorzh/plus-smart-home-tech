package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class ProductInShoppingCartLowQuantityInWarehouse extends ApiException {

    public ProductInShoppingCartLowQuantityInWarehouse(final String logDetails) {
        super(ExceptionReason.PRODUCT_IN_SHOPPING_CART_LOW_QUANTITY_IN_WAREHOUSE, logDetails);
    }
}
