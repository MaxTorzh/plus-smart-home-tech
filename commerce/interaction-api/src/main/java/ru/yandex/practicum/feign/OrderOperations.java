package ru.yandex.practicum.feign;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;

/**
 * Client API interface for managing orders in the online store.
 */
@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderOperations {

    /**
     * Retrieves all orders for a given user.
     * @param username - the authenticated user
     * @return a list of orders associated with the given user
     */
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    List<OrderDto> getUserOrders(@RequestParam @NotBlank String username);

    /**
     * Creates a new order in the system.
     * @param request the request containing shopping cart and delivery address
     * @return the created order
     */
    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    OrderDto createNewOrder(@RequestBody @Valid CreateNewOrderRequest request, @RequestParam @NotBlank String username);

    /**
     * Handles order return request.
     * @param request the request containing order ID and products to return
     * @return the updated order after processing the return
     */
    @PostMapping("/return")
    @ResponseStatus(HttpStatus.OK)
    OrderDto returnProduct(@RequestBody @Valid ProductReturnRequest request);

    /**
     * Handles payment for the order.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to PAID
     */
    @PostMapping("/payment")
    @ResponseStatus(HttpStatus.OK)
    OrderDto processPayment(@RequestBody @NotNull UUID orderId);

    /**
     * Payment failure for the order.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to PAYMENT_FAILED
     */
    @PostMapping("/payment/failed")
    @ResponseStatus(HttpStatus.OK)
    OrderDto markPaymentFailed(@RequestBody @NotNull UUID orderId);

    /**
     * Handles Delivery for the order.
     * @param orderId the UUID of the order
     * @return he updated order with status changed to DELIVERED
     */
    @PostMapping("/delivery")
    @ResponseStatus(HttpStatus.OK)
    OrderDto markAsDelivered(@RequestBody @NotNull UUID orderId);

    /**
     * Marks delivery failure for the order.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to DELIVERY_FAILED
     */
    @PostMapping("/delivery/failed")
    @ResponseStatus(HttpStatus.OK)
    OrderDto markDeliveryFailed(@RequestBody @NotNull UUID orderId);

    /**
     * Marks order as completed.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to COMPLETED
     */
    @PostMapping("/completed")
    @ResponseStatus(HttpStatus.OK)
    OrderDto complete(@RequestBody @NotNull UUID orderId);

    /**
     * Calculates delivery cost for the order.
     * @param orderId the UUID of the order
     * @return the updated order with delivery cost calculated
     */
    @PostMapping("/calculate/delivery")
    @ResponseStatus(HttpStatus.OK)
    OrderDto calculateDeliveryCost(@RequestBody @NotNull UUID orderId);

    /**
     * Calculates total cost of the order.
     * @param orderId the UUID of the order
     * @return the updated order with total cost calculated
     */
    @PostMapping("/calculate/total")
    @ResponseStatus(HttpStatus.OK)
    OrderDto calculateTotalCost(@RequestBody @NotNull UUID orderId);

    /**
     * Marks order as assembled.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to ASSEMBLED
     */
    @PostMapping("/assembly")
    @ResponseStatus(HttpStatus.OK)
    OrderDto assembleOrder(@RequestBody @NotNull UUID orderId);

    /**
     * Mark assembly failure for the order.
     * @param orderId the UUID of the order
     * @return the updated order with status changed to ASSEMBLY_FAILED
     */
    @PostMapping("/assembly/failed")
    @ResponseStatus(HttpStatus.OK)
    OrderDto markAssemblyFailed(@RequestBody @NotNull UUID orderId);
}