package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class NoSpecifiedProductInWarehouseException extends ApiException{

    public NoSpecifiedProductInWarehouseException(final String logDetails) {
        super(ExceptionReason.NO_SPECIFIED_PRODUCT_IN_WAREHOUSE, logDetails);
    }
}
