package ru.yandex.practicum.exception.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import org.springframework.http.HttpStatus;

/**
 * A standard error response format
 */
public record ErrorResponse(
        HttpStatus status,
        String code,
        String message,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime timestamp,
        String cause
) {

    public static ErrorResponse fromException(Exception ex, HttpStatus status, String code) {
        return new ErrorResponse(
                status,
                code,
                ex.getMessage(),
                LocalDateTime.now(),
                (ex.getCause() != null) ? ex.getCause().toString() : "No further details."
        );
    }
}