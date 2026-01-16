package ru.yandex.practicum.exception;

public class ProductInShoppingCartLowQuantityInWarehouse extends ApiException {

    public ProductInShoppingCartLowQuantityInWarehouse(final String logDetails) {
        super(ExceptionReason.PRODUCT_IN_SHOPPING_CART_LOW_QUANTITY_IN_WAREHOUSE, logDetails);
    }
}
