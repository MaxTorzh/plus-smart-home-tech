package ru.yandex.practicum.exception.decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Response;
import feign.codec.ErrorDecoder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import ru.yandex.practicum.exception.ApiException;
import ru.yandex.practicum.exception.ExceptionReason;
import ru.yandex.practicum.exception.dto.ErrorResponse;

/**
 * A custom Feign {@link ErrorDecoder} implementation, to handle errors from Feign Clients. Decodes
 * the given Feign response into an application-specific exception, providing meaningful exception
 * propagation from remote services. Uses Feign default decoder to return a generic fallback
 * exception.
 */
@Slf4j
public class FeignErrorDecoder implements ErrorDecoder {

    private final ErrorDecoder defaultDecoder = new Default();
    private final ObjectMapper objectMapper;

    public FeignErrorDecoder(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Exception decode(final String methodKey, final Response response) {
        final HttpStatus httpStatus = HttpStatus.resolve(response.status());

        if (response.body() == null) {
            log.warn("Feign Client call to {} failed with status {} and empty body.", methodKey,
                    httpStatus);
            return fallbackException(methodKey, response);
        }

        try {
            final ResolvedFeignError feignError = extractFeignErrorDetails(methodKey, response);

            if (feignError.reason != null) {
                log.warn("Feign call to {} failed with code {}: {}",
                        methodKey, feignError.code, feignError.message);
                return new ApiException(feignError.reason,
                        "Feign error from " + methodKey + "details: " + feignError.message);
            } else {
                log.warn("Feign call to {} failed with unknown code '{}': {}",
                        methodKey, feignError.code, feignError.message);
                return fallbackException(methodKey, response);
            }
        } catch (IOException ex) {
            log.error("Feign: failed to decode feign error received from {}: {}", methodKey,
                    ex.getMessage());
            return fallbackException(methodKey, response);
        }

    }

    private ResolvedFeignError extractFeignErrorDetails(final String methodKey,
                                                        final Response response) throws IOException {
        log.debug("Extracting Feign Error details 1 out of response from {}.", methodKey);
        try (InputStream bodyIs = response.body().asInputStream()) {
            final ErrorResponse body = objectMapper.readValue(bodyIs, ErrorResponse.class);
            final String code = body.code();
            final String message = body.message();
            final ExceptionReason reason = resolveReasonByCode(code);
            return new ResolvedFeignError(code, message, reason);
        }
    }

    private ExceptionReason resolveReasonByCode(final String code) {
        log.debug("Resolving exception reason for the exception code {}", code);
        return Arrays.stream(ExceptionReason.values())
                .filter(reason -> reason.getCode().equals(code))
                .findFirst()
                .orElse(null);
    }

    private Exception fallbackException(final String methodKey, final Response response) {
        log.error("Feign default exception. MethodKey:{}, HttpStatus:{}", methodKey, response.status());
        return defaultDecoder.decode(methodKey, response);
    }

    private record ResolvedFeignError(String code, String message, ExceptionReason reason) {

    }
}
