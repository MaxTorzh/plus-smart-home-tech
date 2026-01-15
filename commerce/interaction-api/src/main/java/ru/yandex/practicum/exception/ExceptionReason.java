package ru.yandex.practicum.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Defines the business exception reasons.
 */
@Getter
@AllArgsConstructor
public enum ExceptionReason {

    PRODUCT_NOT_FOUND("No such Product in the store.", HttpStatus.NOT_FOUND),
    NOT_AUTHORIZED_USER("User name should be present.", HttpStatus.UNAUTHORIZED),
    NO_PRODUCTS_IN_SHOPPING_CART("No products in cart.", HttpStatus.BAD_REQUEST),
    SHOPPING_CART_MODIFICATION_NOT_ALLOWED("Cannot modify a deactivated shopping cart.", HttpStatus.FORBIDDEN),
    SPECIFIED_PRODUCT_ALREADY_IN_WAREHOUSE("Product with this description is already registered in the warehouse.", HttpStatus.BAD_REQUEST),
    PRODUCT_IN_SHOPPING_CART_LOW_QUANTITY_IN_WAREHOUSE("The product from the basket is not in the required quantity in the warehouse.", HttpStatus.BAD_REQUEST),
    NO_SPECIFIED_PRODUCT_IN_WAREHOUSE("There is no information about the product in warehouse.", HttpStatus.BAD_REQUEST);

    private final String code;
    private final String message;
    private final HttpStatus status;

    ExceptionReason(final String message, final HttpStatus status) {
        this.code = this.name();
        ;
        this.message = message;
        this.status = status;
    }
}
