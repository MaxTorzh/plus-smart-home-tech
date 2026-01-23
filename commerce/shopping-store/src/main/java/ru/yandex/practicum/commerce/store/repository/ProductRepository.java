package ru.yandex.practicum.commerce.store.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.dto.product.ProductCategory;
import ru.yandex.practicum.commerce.store.model.Product;

import java.util.UUID;

/**
 * Repository interface for managing {@link Product} entities.
 */
public interface ProductRepository extends JpaRepository<Product, UUID> {

    Page<Product> findAllByProductCategory(ProductCategory category, Pageable pageable);
}
