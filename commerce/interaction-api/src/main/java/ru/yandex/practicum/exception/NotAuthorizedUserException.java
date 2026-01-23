package ru.yandex.practicum.exception;

public class NotAuthorizedUserException extends ApiException {

    public NotAuthorizedUserException(final String logDetails) {
        super(ExceptionReason.NOT_AUTHORIZED_USER, logDetails);
    }

}
