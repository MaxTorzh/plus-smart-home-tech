package ru.yandex.practicum.exception;

public class NoOrderFoundException extends ApiException {

    public NoOrderFoundException(final String logDetails) {
        super(ExceptionReason.NO_ORDER_FOUND, logDetails);
    }

}