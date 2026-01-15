package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.UNAUTHORIZED)
public class NotAuthorizedUserException extends ApiException {

    public NotAuthorizedUserException(final String logDetails) {
        super(ExceptionReason.NOT_AUTHORIZED_USER, logDetails);
    }

}
