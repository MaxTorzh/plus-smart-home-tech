package ru.yandex.practicum.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;

/**
 * Client API interface for the operations of the online store warehouse.
 */
@FeignClient(name = "warehouse", path = "/api/v1/warehouse")
public interface WarehouseOperations {

    /**
     * Registers new product details to the warehouse.
     *
     * @param product the product data transfer object containing product details
     */
    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    void addProduct(@Validated @RequestBody NewProductInWarehouseRequest product);

    /**
     * Pre-checks whether the warehouse has sufficient stock for the given shopping cart.
     *
     * @param shoppingCart the shopping cart containing the requested products
     * @return a summary of the reserved products for the shopping cart
     */
    @PutMapping("/check")
    @ResponseStatus(HttpStatus.OK)
    BookedProductsDto checkStock(@Validated @RequestBody ShoppingCartDto shoppingCart);

    /**
     * Increases the quantity of a specific product in the warehouse.
     *
     * @param request the request to increase the quantity of a product by its identifier
     */
    @PutMapping("/add")
    @ResponseStatus(HttpStatus.OK)
    void increaseProductQuantity(@Validated @RequestBody AddProductToWarehouseRequest request);

    /**
     * Retrieves the warehouse address for delivery calculation.
     *
     * @return the warehouse address
     */
    @GetMapping("/address")
    @ResponseStatus(HttpStatus.OK)
    AddressDto getWarehouseAddress();
}
