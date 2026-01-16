package ru.yandex.practicum.dto.product;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Optional;

public enum QuantityState {
    ENDED,
    FEW,
    ENOUGH,
    MANY;

    @JsonCreator
    public static QuantityState fromString(String value) {
        if (value == null) return null;
        try {
            return QuantityState.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown quantity state: '" + value +
                            "'. Must be one of: ENDED, FEW, ENOUGH, MANY");
        }
    }

    @JsonValue
    public String toValue() {
        return this.name();
    }

    public static Optional<QuantityState> from(final String stringState) {
        return Arrays.stream(values())
                .filter(state -> state.name().equalsIgnoreCase(stringState))
                .findFirst();
    }
}
