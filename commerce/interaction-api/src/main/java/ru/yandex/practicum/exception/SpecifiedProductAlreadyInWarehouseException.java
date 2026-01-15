package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class SpecifiedProductAlreadyInWarehouseException extends ApiException {

    public SpecifiedProductAlreadyInWarehouseException(final String logDetails) {
        super(ExceptionReason.SPECIFIED_PRODUCT_ALREADY_IN_WAREHOUSE, logDetails);
    }
}
