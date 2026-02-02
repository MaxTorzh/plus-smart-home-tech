package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

/**
 * Client API interface for simulating delivery operations.
 */
@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryOperations {

    /**
     * Registers a new delivery in the system.
     * @param deliveryDto delivery information to be saved
     * @return saved delivery with assigned ID
     */
    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    DeliveryDto planDelivery(@RequestBody @Valid DeliveryDto deliveryDto);

    /**
     * Simulates a successful delivery confirmation.
     * @param orderId the ID of the associated order
     */
    @PostMapping("/successful")
    @ResponseStatus(HttpStatus.OK)
    void confirmDelivery(@RequestBody UUID orderId);


    /**
     * Simulates the event of order being picked up for delivery.
     * @param orderId the ID of the associated order
     */
    @PostMapping("/picked")
    @ResponseStatus(HttpStatus.OK)
    void markDeliveryPicked(@RequestBody UUID orderId);

    /**
     * Simulates a delivery failure event.
     * @param orderId the ID of the associated order
     */
    @PostMapping("/failed")
    @ResponseStatus(HttpStatus.OK)
    void markDeliveryFailed(@RequestBody UUID orderId);

    /**
     * Calculates the delivery cost for the given order.
     * @param orderDto the order details
     * @return calculated delivery cost
     */
    @PostMapping("/cost")
    @ResponseStatus(HttpStatus.OK)
    BigDecimal calculateDeliveryCost(@RequestBody @Valid OrderDto orderDto);

}
