package ru.yandex.practicum.exception;

public class DeliveryForSpecifiedOrderAlreadyExists extends ApiException {

    public DeliveryForSpecifiedOrderAlreadyExists(final String logDetails) {
        super(ExceptionReason.DELIVERY_FOR_SPECIFIED_ORDER_ALREADY_EXIST, logDetails);
    }
}
