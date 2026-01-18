package ru.yandex.practicum.dto.product;

import java.util.Arrays;
import java.util.Optional;

/**
 * Possible states of the degree of product availability.
 * <p>
 * <ul>
 *   <li>{@link #ENDED} - Product is out of stock; </li>
 *   <li>{@link #FEW} - Fewer than 10 units available; </li>
 *   <li>{@link #ENOUGH} - Between 10 and 100 units available; </li>
 *   <li>{@link #MANY} - More than 100 units available. </li>
 * </ul>
 */
public enum QuantityState {

    ENDED,
    FEW,
    ENOUGH,
    MANY;

    public static Optional<QuantityState> from(final String stringState) {
        return Arrays.stream(values())
                .filter(state -> state.name().equalsIgnoreCase(stringState))
                .findFirst();
    }
}
