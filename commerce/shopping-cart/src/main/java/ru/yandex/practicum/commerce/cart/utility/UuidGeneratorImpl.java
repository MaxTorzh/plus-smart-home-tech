package ru.yandex.practicum.commerce.cart.utility;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Implementation of {@link UuidGenerator} that generates random UUIDs. Generates a new random UUID
 * and logs the generated ID.
 */
@Component
@Slf4j
public class UuidGeneratorImpl implements UuidGenerator {

    @Override
    public UUID generate() {
        final UUID id = UUID.randomUUID();
        log.debug("Generated ID: {}", id);
        return id;
    }
}
