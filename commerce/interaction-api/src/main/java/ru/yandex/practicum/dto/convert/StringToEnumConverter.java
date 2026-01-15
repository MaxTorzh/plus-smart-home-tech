package ru.yandex.practicum.dto.converter;

import java.util.Arrays;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

/**
 * A generic enum converter, allows to convert a String into any Enum Type, case-insensitively.
 */
@Data
@Slf4j
public class StringToEnumConverter<T extends Enum<T>> implements Converter<String, T> {

    private final Class<T> enumType;

    @Override
    public T convert(@NonNull String source) {
        log.debug("Converting String {} into enum of type {}.", source, enumType);
        return Arrays.stream(enumType.getEnumConstants())
                .filter((e -> e.name().equalsIgnoreCase(source)))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Invalid enum value: " + source + " for " + enumType.getSimpleName()));
    }
}
