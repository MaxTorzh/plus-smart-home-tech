package ru.yandex.practicum.commerce.store.utility;

import java.util.UUID;

/**
 * Interface for generating unique UUIDs.
 */
public interface UuidGenerator {

    UUID generate();
}