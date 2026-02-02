package ru.yandex.practicum.exception;

public class PaymentForSpecifiedOrderAlreadyExists extends ApiException {

    public PaymentForSpecifiedOrderAlreadyExists(String logDetails) {
        super(ExceptionReason.PAYMENT_FOR_SPECIFIED_ORDER_ALREADY_EXIST, logDetails);
    }
}

