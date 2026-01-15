package ru.yandex.practicum.exception.policy;

import org.springframework.http.HttpStatus;

/**
 * A contract for custom exceptions in order to have a standardized behavior across the API.
 * Provides some of the required properties for the error response schema.
 */
public interface ExceptionPolicy {

    /**
     * @return the business-specific error code
     */
    String getCode();

    /**
     * @return the user-friendlyerror message
     */
    String getMessage();

    /**
     * @return the HTTP status code associated with the exception
     */
    HttpStatus getHttpStatus();
}
