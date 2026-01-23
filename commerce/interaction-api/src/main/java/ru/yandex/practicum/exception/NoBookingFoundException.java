package ru.yandex.practicum.exception;

public class NoBookingFoundException extends ApiException {

    public NoBookingFoundException(String logDetails) {
        super(ExceptionReason.NO_BOOKING_FOUND, logDetails);

    }
}
