package ru.yandex.practicum.exception;

import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;
import ru.yandex.practicum.exception.policy.ExceptionPolicy;

/**
 * Base class for all custom API exceptions that implements {@link ExceptionPolicy} and provides
 * structured error information.
 */
@Getter
@Setter
public class ApiException extends RuntimeException implements ExceptionPolicy {

    protected final String code;
    protected final String message;
    protected final HttpStatus httpStatus;
    protected final String debugMessage;

    public ApiException(final ExceptionReason reason, final String debugMessage) {
        super(reason.getMessage());
        this.code = reason.getCode();
        this.message = reason.getMessage();
        this.httpStatus = reason.getStatus();
        this.debugMessage = debugMessage;
    }

    public ApiException(final ExceptionReason reason, final HttpStatus overridingStatus,
                        final String debugMessage) {
        super(reason.getMessage());
        this.code = reason.getCode();
        this.message = reason.getMessage();
        this.httpStatus = overridingStatus;
        this.debugMessage = debugMessage;
    }


    @Override
    public String getLocalizedMessage() {
        return getMessage();
    }
}
