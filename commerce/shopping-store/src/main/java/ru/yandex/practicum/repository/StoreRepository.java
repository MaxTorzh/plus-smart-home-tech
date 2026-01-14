package ru.yandex.practicum.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.types.ProductCategory;

import java.util.List;
import java.util.Optional;

public interface StoreRepository extends JpaRepository<Product, Long> {

    Optional<Product> findByProductId(String productId);

    List<Product> findAllByProductCategory(ProductCategory category, Pageable pageable);
}
