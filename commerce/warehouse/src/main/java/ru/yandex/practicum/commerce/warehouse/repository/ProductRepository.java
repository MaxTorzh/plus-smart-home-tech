package ru.yandex.practicum.commerce.warehouse.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.warehouse.model.Product;

public interface ProductRepository extends JpaRepository<Product, UUID> {

}

