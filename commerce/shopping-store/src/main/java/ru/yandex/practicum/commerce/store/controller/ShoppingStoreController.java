package ru.yandex.practicum.commerce.store.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.product.*;
import ru.yandex.practicum.feign.ShoppingStoreOperations;
import ru.yandex.practicum.commerce.store.service.ShoppingStoreService;

import java.util.UUID;

/**
 * REST controller for managing products in the shopping store.
 */
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@Slf4j
@Validated
public class ShoppingStoreController implements ShoppingStoreOperations {

    private final ShoppingStoreService shoppingStoreService;

    /**
     * Retrieves a paginated list of products filtered by a specific category.
     *
     * @param category the product category
     * @param pageable pagination parameters
     * @return a page of products
     */
    @Override
    public ProductPage getProductsByCategory(final ProductCategory category,
                                             final Pageable pageable) {
        log.info("Received request to fetch products for the category: {}, with pagination: {}.",
                category, pageable);
        final Page<ProductDto> response = shoppingStoreService.getProductsByCategory(category,
                pageable);
        log.info("Returning product list of size: {}", response.getTotalElements());
        return new ProductPage(response);
    }

    /**
     * Retrieves product details by its ID.
     *
     * @param productId the product ID
     * @return the product details
     */
    @Override
    public ProductDto getProductById(final UUID productId) {
        log.info("Received request to retrieve data of the product with ID: {}.", productId);
        final ProductDto product = shoppingStoreService.getProductById(productId);
        log.info("Returning details of the product {}.", product.getProductName());
        return product;
    }

    /**
     * Creates a new product in the store.
     *
     * @param productDto the product details
     * @return the created product
     */
    @Override
    public ProductDto addProduct(final ProductDto productDto) {
        log.info("Request received to add a new product: {}.", productDto.getProductName());
        final ProductDto savedProduct = shoppingStoreService.addProduct(productDto);
        log.info("Product {} saved with ID {}.", savedProduct.getProductName(),
                savedProduct.getProductId());
        return savedProduct;
    }

    /**
     * Updates an existing product.
     *
     * @param productDto the updated product details
     * @return the updated product
     */
    @Override
    public ProductDto updateProduct(final ProductDto productDto) {
        log.info("Request received to update a product with ID {}.", productDto.getProductId());
        final ProductDto updatedProduct = shoppingStoreService.updateProduct(productDto);
        log.info("Product, ID {} updated successfully.", updatedProduct.getProductId());
        return updatedProduct;
    }

    /**
     * Updates the quantity status of a product in the store. (The API is called from the warehouse
     * side.)
     * @param productId     the unique identifier of the product
     * @param quantityState the new quantity state to be set for the product
     * @return true if the update was successful
     */
    @Override
    public boolean updateQuantityState(final UUID productId, final QuantityState quantityState) {
        final SetProductQuantityStateRequest request = new SetProductQuantityStateRequest(productId,
                quantityState);
        log.info("Request received to update quantity state for the product with ID {}.",
                request.getProductId());
        boolean isUpdated = shoppingStoreService.updateQuantityState(request);
        log.info("Product quantity state updated successfully.");
        return isUpdated;
    }

    /**
     * Removes a product from the store's assortment. (Management staff function)
     *
     * @param productId the ID of the product to be removed
     * @return true if the product was successfully removed
     */
    @Override
    @PutMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public boolean removeProductFromStore(final UUID productId) {
        log.info("Request received to remove product with ID {} from the store.", productId);
        boolean isRemoved = shoppingStoreService.removeProduct(productId);
        log.info("Product state updated successfully to 'DEACTIVATE'.");
        return isRemoved;
    }
}