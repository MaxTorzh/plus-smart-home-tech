package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.StoreRepository;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.SetProductCountState;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.types.ProductState;
import ru.yandex.practicum.types.QuantityState;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class StoreServiceImpl implements StoreService {
    private final StoreRepository storeRepository;
    private final ProductMapper productMapper;

    @Override
    public List<ProductDto> getProductsByCategory(String category, Pageable pageable) {
        try {
            ru.yandex.practicum.types.ProductCategory productCategory =
                    ru.yandex.practicum.types.ProductCategory.valueOf(category.toUpperCase());
            List<Product> products = storeRepository.findAllByProductCategory(productCategory, pageable);
            return productMapper.toProductDtoList(products);
        } catch (IllegalArgumentException e) {
            throw new NotFoundException("Category not found: " + category);
        }
    }

    @Override
    public List<ProductDto> getAllProducts(Pageable pageable) {
        List<Product> products = storeRepository.findAll(pageable).getContent();
        return productMapper.toProductDtoList(products);
    }

    @Transactional
    @Override
    public ProductDto createProduct(ProductDto productDto) {
        Optional<Product> existingProduct = storeRepository.findByProductId(productDto.getProductId());
        if (existingProduct.isPresent()) {
            return productMapper.toProductDto(existingProduct.get());
        }

        Product product = productMapper.toProduct(productDto);

        if (product.getProductState() == null) {
            product.setProductState(ProductState.ACTIVE);
        }
        if (product.getQuantityState() == null) {
            product.setQuantityState(QuantityState.ENOUGH);
        }

        Product savedProduct = storeRepository.save(product);
        return productMapper.toProductDto(savedProduct);
    }

    @Transactional
    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        Product existingProduct = storeRepository.findByProductId(productDto.getProductId())
                .orElseThrow(() -> new NotFoundException("Product not found: " + productDto.getProductId()));

        if (productDto.getProductName() != null) {
            existingProduct.setProductName(productDto.getProductName());
        }
        if (productDto.getProductCategory() != null) {
            existingProduct.setProductCategory(productDto.getProductCategory());
        }
        if (productDto.getPrice() > 0) {
            existingProduct.setPrice(productDto.getPrice());
        }
        if (productDto.getQuantityState() != null) {
            existingProduct.setQuantityState(productDto.getQuantityState());
        }
        if (productDto.getProductState() != null) {
            existingProduct.setProductState(productDto.getProductState());
        }

        Product updatedProduct = storeRepository.save(existingProduct);
        return productMapper.toProductDto(updatedProduct);
    }

    @Transactional
    @Override
    public ProductDto removeProduct(String productId) {
        Product product = storeRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Product not found: " + productId));

        product.setProductState(ProductState.DEACTIVATE);
        Product updatedProduct = storeRepository.save(product);
        return productMapper.toProductDto(updatedProduct);
    }

    @Transactional
    @Override
    public ProductDto changeState(SetProductCountState request) {
        Product product = storeRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new NotFoundException("Product not found: " + request.getProductId()));

        product.setQuantityState(request.getQuantityState());
        Product updatedProduct = storeRepository.save(product);
        return productMapper.toProductDto(updatedProduct);
    }

    @Override
    public ProductDto getInfoByProduct(String productId) {
        Product product = storeRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Product not found: " + productId));
        return productMapper.toProductDto(product);
    }
}