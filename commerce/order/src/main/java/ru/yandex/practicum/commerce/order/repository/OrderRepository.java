package ru.yandex.practicum.commerce.order.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.order.model.Order;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    List<Order> findAllByUsername(String username);

}
