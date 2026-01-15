package ru.yandex.practicum.commerce.store.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.ProductState;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.store.mapper.ProductMapper;
import ru.yandex.practicum.commerce.store.model.Product;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;
import ru.yandex.practicum.commerce.store.utility.UuidGenerator;

import java.util.UUID;

/**
 * Implementation of {@link ShoppingStoreService} for managing products in the shopping store.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShoppingStoreServiceImpl implements ShoppingStoreService {

    private final ProductRepository productRepository;
    private final UuidGenerator uuidGenerator;

    @Transactional(readOnly = true)
    @Override
    public Page<ProductDto> getProductsByCategory(final ProductCategory category,
                                                  final Pageable pageable) {
        log.debug("Retrieving products for category: {}, page: {}, size: {}, sortBy: {}...",
                category, pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());
        final PageRequest page = PageRequest.of(pageable.getPageNumber(), pageable.getPageSize(),
                pageable.getSort());
        final Page<Product> products = productRepository.findAllByProductCategory(category, page);
        return products.map(ProductMapper::toDto);
    }

    @Transactional(readOnly = true)
    @Override
    public ProductDto getProductById(final UUID productId) {
        log.debug("Retrieving details of the product with ID {}...", productId);
        final Product product = getProductOrThrow(productId);
        log.debug("Successfully retrieved product: {}", product);
        return ProductMapper.toDto(product);
    }

    @Transactional
    @Override
    public ProductDto addProduct(final ProductDto productDto) {
        log.debug("Saving new product info to the DB: {}...", productDto);
        setId(productDto);
        final Product productToSave = ProductMapper.toEntity(productDto);
        final Product savedProduct = productRepository.save(productToSave);
        log.debug("New product saved successfully with ID: {}.", savedProduct.getProductId());
        return ProductMapper.toDto(savedProduct);
    }

    @Transactional
    @Override
    public ProductDto updateProduct(final ProductDto productDto) {
        log.debug("Updating a product info with details: {}...", productDto);
        final Product product = getProductOrThrow(productDto.getProductId());
        updateProductContent(product, productDto);
        final Product updatedProduct = productRepository.save(product);
        log.debug("Updated product: {}.", updatedProduct);
        return ProductMapper.toDto(updatedProduct);
    }

    @Transactional
    @Override
    public boolean updateQuantityState(final SetProductQuantityStateRequest request) {
        log.debug("Updating quantity state for the product ID {} - {}.",
                request.getProductId(), request.getQuantityState());
        final Product product = getProductOrThrow(request.getProductId());
        product.setQuantityState(request.getQuantityState());
        final Product updatedProduct = productRepository.save(product);
        log.debug("Updated quantity state for the product ID {} - {}.",
                updatedProduct.getProductId(), updatedProduct.getQuantityState());
        return true;
    }

    @Transactional
    @Override
    public boolean removeProduct(final UUID productId) {
        log.debug("Setting product with ID {} with the state 'DEACTIVATE'.", productId);
        final Product product = getProductOrThrow(productId);
        product.setProductState(ProductState.DEACTIVATE);
        final Product updatedProduct = productRepository.save(product);
        log.debug("Updated product state: {}.", updatedProduct);
        return true;
    }

    private void updateProductContent(final Product target, final ProductDto source) {
        target.setProductName(
                source.getProductName() != null ? source.getProductName() : target.getProductName());
        target.setDescription(
                source.getDescription() != null ? source.getDescription() : target.getDescription());
        target.setImageSrc(source.getImageSrc() != null ? source.getImageSrc() : target.getImageSrc());
        target.setQuantityState(
                source.getQuantityState() != null ? source.getQuantityState() : target.getQuantityState());
        target.setProductState(
                source.getProductState() != null ? source.getProductState() : target.getProductState());
        target.setProductCategory(source.getProductCategory() != null ? source.getProductCategory()
                : target.getProductCategory());
        target.setPrice(source.getPrice() != null ? source.getPrice() : target.getPrice());
    }

    private void setId(final ProductDto productDto) {
        if (productDto.getProductId() == null) {
            productDto.setProductId(uuidGenerator.generate());
        }
    }

    private Product getProductOrThrow(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("ID: " + productId));
    }
}