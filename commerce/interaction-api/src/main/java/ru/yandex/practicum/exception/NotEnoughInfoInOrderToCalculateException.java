package ru.yandex.practicum.exception;

public class NotEnoughInfoInOrderToCalculateException extends ApiException {

    public NotEnoughInfoInOrderToCalculateException(String logDetail) {
        super(ExceptionReason.NOT_ENOUGH_ORDER_INFO_TO_CALCULATE, logDetail);
    }
}
