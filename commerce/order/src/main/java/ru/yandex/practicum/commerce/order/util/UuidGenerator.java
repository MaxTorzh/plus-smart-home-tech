package ru.yandex.practicum.commerce.order.util;

import java.util.UUID;

/**
 * Interface for generating unique UUIDs.
 */
public interface UuidGenerator {

    UUID generate();

}