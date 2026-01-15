package ru.yandex.practicum.exception.handler;

import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import ru.yandex.practicum.exception.ApiException;
import ru.yandex.practicum.exception.dto.ErrorResponse;

/**
 * Global API exception handler responsible for catching any uncaught {@link Exception} and
 * converting it into standardized {@link ErrorResponse} JSON responses.
 */
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApiException(final ApiException ex) {
        log.warn("API Exception: {} - {}", ex.getMessage(), ex.getDebugMessage(), ex);

        final ErrorResponse response = ErrorResponse.fromException(
                ex,
                ex.getHttpStatus(),
                ex.getCode());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUncaughtException(final Exception ex) {
        log.warn("Unexpected error: {}", ex.getMessage(), ex);

        final ErrorResponse response = new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Something went wrong. Please contact support if the problem persists.",
                LocalDateTime.now(),
                "Refer to server logs for details."
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
