package ru.yandex.practicum.feign;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.ProductPage;
import ru.yandex.practicum.dto.product.QuantityState;

/**
 * Client API interface for managing products in the online store.
 */
@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface ShoppingStoreOperations {

    /**
     * Retrieves a paginated list of products based on the specified category.
     *
     * @param category the product category
     * @param pageable pagination details, defaults to page 0, size 10, sorted by product name
     * @return a paginated list of products within the specified category
     */
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    ProductPage getProductsByCategory(
            @RequestParam("category") ProductCategory category,
            @PageableDefault(page = 0, size = 10, sort = "productName") Pageable pageable);

    /**
     * Retrieves the details of a specific product by its ID.
     *
     * @param productId the unique identifier of the product
     * @return the product details
     */
    @GetMapping("/{productId}")
    @ResponseStatus(HttpStatus.OK)
    ProductDto getProductById(@PathVariable("productId") UUID productId);

    /**
     * Save a new product details to the store.
     *
     * @param productDto the product data transfer object containing product details
     * @return the newly added product
     */
    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    ProductDto addProduct(@Validated @RequestBody ProductDto productDto);

    /**
     * Updates an existing product in the store.
     *
     * @param productDto the product data transfer object containing updated product details
     * @return the updated product
     */
    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    ProductDto updateProduct(@Validated @RequestBody ProductDto productDto);

    /**
     * Updates the quantity state of a product in the store.
     *
     * the request object containing the product ID and the new quantity state
     * @return {@code true} if the update was successful, otherwise {@code false}
     */
    @PutMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    boolean updateQuantityState(@RequestParam("productId") @NotNull UUID productId,
                                @RequestParam("quantityState") @NotNull QuantityState quantityState);

    /**
     * Removes a product from the store. Marks a product as {@code DEACTIVATED} instead of physically
     * removing it from the database. The product remains stored for historical data purposes.
     *
     * @param productId the unique identifier of the product to be removed
     * @return {@code true} if the removal was successful, otherwise {@code false}
     */
    @PutMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    boolean removeProductFromStore(@RequestBody UUID productId);
}
