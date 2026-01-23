package ru.yandex.practicum.exception;

public class SpecifiedProductAlreadyInWarehouseException extends ApiException {

    public SpecifiedProductAlreadyInWarehouseException(final String logDetails) {
        super(ExceptionReason.SPECIFIED_PRODUCT_ALREADY_IN_WAREHOUSE, logDetails);
    }
}
