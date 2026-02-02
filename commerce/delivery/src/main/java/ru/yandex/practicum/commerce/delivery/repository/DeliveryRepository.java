package ru.yandex.practicum.commerce.delivery.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.delivery.model.Delivery;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    boolean existsByOrderId(UUID orderId);

    Optional<Delivery> findByOrderId(UUID orderId);
}
