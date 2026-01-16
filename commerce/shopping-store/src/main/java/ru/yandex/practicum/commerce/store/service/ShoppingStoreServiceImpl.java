package ru.yandex.practicum.commerce.store.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.ProductState;
import ru.yandex.practicum.dto.product.QuantityState;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.store.mapper.ProductMapper;
import ru.yandex.practicum.commerce.store.model.Product;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;
import ru.yandex.practicum.commerce.store.utility.UuidGenerator;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShoppingStoreServiceImpl implements ShoppingStoreService {

    private final ProductRepository productRepository;
    private final UuidGenerator uuidGenerator;

    @Transactional(readOnly = true)
    @Override
    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        log.debug("Getting products by category: {}, pageable: {}", category, pageable);
        return productRepository.findAllByProductCategory(category, pageable)
                .map(ProductMapper::toDto);
    }

    @Transactional(readOnly = true)
    @Override
    public Page<ProductDto> getAllProducts(Pageable pageable) {
        log.debug("Getting all products with pageable: {}", pageable);
        return productRepository.findAll(pageable)
                .map(ProductMapper::toDto);
    }

    @Transactional(readOnly = true)
    @Override
    public ProductDto getProductById(UUID productId) {
        log.debug("Getting product by ID: {}", productId);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found: " + productId));
        return ProductMapper.toDto(product);
    }

    @Transactional
    @Override
    public ProductDto addProduct(ProductDto productDto) {
        log.debug("Adding new product: {}", productDto);

        if (productDto.getProductId() == null) {
            productDto.setProductId(uuidGenerator.generate());
        }

        if (productRepository.existsById(productDto.getProductId())) {
            throw new IllegalArgumentException("Product with ID " + productDto.getProductId() + " already exists");
        }

        validateProductDto(productDto);

        setDefaultValues(productDto);

        Product product = ProductMapper.toEntity(productDto);
        Product savedProduct = productRepository.save(product);
        log.info("Product created with ID: {}", savedProduct.getProductId());
        return ProductMapper.toDto(savedProduct);
    }

    @Transactional
    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        log.debug("Updating product: {}", productDto);

        if (productDto.getProductId() == null) {
            throw new IllegalArgumentException("Product ID is required for update");
        }

        Product product = productRepository.findById(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Product not found: " + productDto.getProductId()));

        updateProductFields(product, productDto);

        Product updatedProduct = productRepository.save(product);
        log.info("Product updated with ID: {}", updatedProduct.getProductId());
        return ProductMapper.toDto(updatedProduct);
    }

    public ProductDto updateQuantityState(SetProductQuantityStateRequest request) {
        log.debug("Updating quantity state: {}", request);
        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found: " + request.getProductId()));

        if (request.getQuantityState() == null) {
            throw new IllegalArgumentException("Quantity state cannot be null");
        }
        product.setQuantityState(request.getQuantityState());

        Product updatedProduct = productRepository.save(product);
        log.info("Quantity state updated for product ID: {}", product.getProductId());

        return ProductMapper.toDto(updatedProduct);
    }

    @Transactional
    @Override
    public boolean removeProduct(UUID productId) {
        log.debug("Removing product: {}", productId);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found: " + productId));

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Product deactivated with ID: {}", product.getProductId());
        return true;
    }

    private void validateProductDto(ProductDto productDto) {
        if (productDto.getProductName() == null || productDto.getProductName().isBlank()) {
            throw new IllegalArgumentException("Product name is required");
        }
        if (productDto.getDescription() == null || productDto.getDescription().isBlank()) {
            throw new IllegalArgumentException("Description is required");
        }
        if (productDto.getPrice() == null || productDto.getPrice().compareTo(BigDecimal.ONE) < 0) {
            throw new IllegalArgumentException("Price must be at least 1");
        }
        if (productDto.getQuantityState() == null) {
            throw new IllegalArgumentException("Quantity state is required");
        }
        if (productDto.getProductState() == null) {
            throw new IllegalArgumentException("Product state is required");
        }
    }

    private void setDefaultValues(ProductDto productDto) {
        if (productDto.getProductState() == null) {
            productDto.setProductState(ProductState.ACTIVE);
        }
        if (productDto.getQuantityState() == null) {
            productDto.setQuantityState(QuantityState.ENOUGH);
        }
        if (productDto.getRating() == null) {
            productDto.setRating(BigDecimal.ZERO.setScale(1));
        }
    }

    private void updateProductFields(Product product, ProductDto productDto) {
        if (productDto.getProductName() != null && !productDto.getProductName().isBlank()) {
            product.setProductName(productDto.getProductName());
        }
        if (productDto.getDescription() != null && !productDto.getDescription().isBlank()) {
            product.setDescription(productDto.getDescription());
        }
        if (productDto.getImageSrc() != null) {
            product.setImageSrc(productDto.getImageSrc());
        }
        if (productDto.getQuantityState() != null) {
            product.setQuantityState(productDto.getQuantityState());
        }
        if (productDto.getProductState() != null) {
            product.setProductState(productDto.getProductState());
        }
        if (productDto.getProductCategory() != null) {
            product.setProductCategory(productDto.getProductCategory());
        }
        if (productDto.getPrice() != null) {
            if (productDto.getPrice().compareTo(BigDecimal.ONE) < 0) {
                throw new IllegalArgumentException("Price must be at least 1");
            }
            product.setPrice(productDto.getPrice());
        }
        if (productDto.getRating() != null) {
            if (productDto.getRating().compareTo(BigDecimal.ZERO) < 0 ||
                    productDto.getRating().compareTo(new BigDecimal("5.0")) > 0) {
                throw new IllegalArgumentException("Rating must be between 0.0 and 5.0");
            }
            product.setRating(productDto.getRating());
        }
    }
}