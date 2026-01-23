package ru.yandex.practicum.exception;

public class NoSpecifiedProductInWarehouseException extends ApiException{

    public NoSpecifiedProductInWarehouseException(final String logDetails) {
        super(ExceptionReason.NO_SPECIFIED_PRODUCT_IN_WAREHOUSE, logDetails);
    }
}
