package ru.yandex.practicum.commerce.warehouse.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.warehouse.model.Booking;

public interface BookingRepository extends JpaRepository<Booking, UUID> {

    Optional<Booking> findByOrderId(UUID orderId);
}
