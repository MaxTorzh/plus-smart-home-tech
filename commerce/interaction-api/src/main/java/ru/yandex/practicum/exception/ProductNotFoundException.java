package ru.yandex.practicum.exception;

public class ProductNotFoundException extends ApiException {

    public ProductNotFoundException(final String logDetails) {
        super(ExceptionReason.PRODUCT_NOT_FOUND, logDetails);
    }
}
