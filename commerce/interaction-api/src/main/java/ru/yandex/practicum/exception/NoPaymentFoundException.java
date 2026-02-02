package ru.yandex.practicum.exception;

public class NoPaymentFoundException extends ApiException {

    public NoPaymentFoundException(final String logDetails) {
        super(ExceptionReason.NO_PAYMENT_FOUND, logDetails);
    }

}