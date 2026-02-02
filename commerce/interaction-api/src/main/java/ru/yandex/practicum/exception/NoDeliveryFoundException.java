package ru.yandex.practicum.exception;

public class NoDeliveryFoundException extends ApiException {

    public NoDeliveryFoundException(final String logDetails) {
        super(ExceptionReason.NO_DELIVERY_FOUND, logDetails);
    }

}

