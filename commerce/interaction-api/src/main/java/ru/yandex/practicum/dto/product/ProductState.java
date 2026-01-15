package ru.yandex.practicum.dto.product;

/**
 * Possible states of a product.
 * <p>
 * <ul>
 *   <li>{@link #ACTIVE} - The product is available for purchase and visible to customers; </li>
 *   <li>{@link #DEACTIVATE} - The product is no longer available for purchase but remains in the system for historical records.</li>
 * </ul>
 */
public enum ProductState {

    ACTIVE,
    DEACTIVATE
}
