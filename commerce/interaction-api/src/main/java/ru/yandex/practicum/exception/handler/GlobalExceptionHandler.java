package ru.yandex.practicum.exception.handler;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import ru.yandex.practicum.exception.ApiException;
import ru.yandex.practicum.exception.dto.ErrorResponse;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

/**
 * Global API exception handler responsible for catching any uncaught {@link Exception} and
 * converting it into standardized {@link ErrorResponse} JSON responses.
 */
@Slf4j
@ControllerAdvice
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

    @ExceptionHandler(FeignException.class)
    public ResponseEntity<ErrorResponse> handleFeignException(final FeignException ex) {
        log.warn("Feign Client Exception: {} - Status: {}", ex.getMessage(), ex.status(), ex);

        // Безопасное получение HttpStatus
        HttpStatus status;
        try {
            status = ex.status() > 0 ? HttpStatus.valueOf(ex.status())
                    : HttpStatus.INTERNAL_SERVER_ERROR;
        } catch (IllegalArgumentException e) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        final ErrorResponse response = new ErrorResponse(
                status,
                "FEIGN_CLIENT_ERROR",
                "Service temporarily unavailable",
                LocalDateTime.now(),
                ex.contentUTF8()
        );

        return ResponseEntity.status(status).body(response);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            final MethodArgumentNotValidException ex) {

        log.warn("Validation error: {}", ex.getMessage(), ex);

        String errorMessage = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));

        final ErrorResponse response = new ErrorResponse(
                HttpStatus.BAD_REQUEST,
                "VALIDATION_ERROR",
                errorMessage,
                LocalDateTime.now(),
                "Invalid request parameters"
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUncaughtException(final Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);

        final ErrorResponse response = new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Something went wrong. Please contact support if the problem persists.",
                LocalDateTime.now(),
                "Refer to server logs for details."
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupported(
            HttpRequestMethodNotSupportedException ex) {

        log.warn("HTTP Method not supported: {}", ex.getMessage());

        String supportedMethods = "";
        if (ex.getSupportedHttpMethods() != null) {
            supportedMethods = ex.getSupportedHttpMethods().stream()
                    .map(HttpMethod::name)  // Используем HttpMethod::name
                    .collect(Collectors.joining(", "));
        }

        final ErrorResponse response = new ErrorResponse(
                HttpStatus.METHOD_NOT_ALLOWED,
                "METHOD_NOT_ALLOWED",
                "HTTP method " + ex.getMethod() + " is not supported for this endpoint",
                LocalDateTime.now(),
                "Supported methods: " + supportedMethods
        );

        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(response);
    }
}