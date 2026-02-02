package ru.yandex.practicum.commerce.delivery.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.delivery.model.Address;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.dto.order.OrderDto;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeliveryCostCalculator {

    private static final BigDecimal BASE_COST = BigDecimal.valueOf(5.0);
    private static final BigDecimal ADDRESS_1_COEFFICIENT = BigDecimal.valueOf(1.0);
    private static final BigDecimal ADDRESS_2_COEFFICIENT = BigDecimal.valueOf(2.0);
    private static final BigDecimal FRAGILE_COEFFICIENT = BigDecimal.valueOf(0.2);
    private static final BigDecimal WEIGHT_COEFFICIENT = BigDecimal.valueOf(0.3);
    private static final BigDecimal VOLUME_COEFFICIENT = BigDecimal.valueOf(0.2);
    private static final BigDecimal DISTANCE_COEFFICIENT = BigDecimal.valueOf(0.2);
    private static final int SCALE = 2;

    /**
     * Calculates the delivery cost for the given order.
     * <p>
     * The cost depends on:
     * <ul>
     *     <li>Warehouse address (affects base multiplier)</li>
     *     <li>Delivery destination street</li>
     *     <li>Fragility of goods</li>
     *     <li>Order weight and volume</li>
     * </ul>
     *
     * @param order the order details
     * @return calculated delivery cost
     */

    public BigDecimal calculate(final OrderDto order, final Delivery delivery) {
        log.debug("Calculating delivery cost for the order {} with delivery data {}.",
                order, delivery);

        BigDecimal deliveryCost = BASE_COST;

        deliveryCost = deliveryCost.add(calculateWarehouseCostComponent(deliveryCost, delivery.getFromAddress()));
        deliveryCost = deliveryCost.add(calculateFragileCostComponent(deliveryCost, order.isFragile()));
        deliveryCost = deliveryCost.add(calculateWeightCostComponent(BigDecimal.valueOf(order.getDeliveryWeight())));
        deliveryCost = deliveryCost.add(calculateVolumeCostComponent(BigDecimal.valueOf(order.getDeliveryVolume())));
        deliveryCost = deliveryCost.add(calculateDistanceCostComponent(deliveryCost, delivery.getFromAddress(), delivery.getToAddress()));

        return deliveryCost;
    }

    private BigDecimal calculateDistanceCostComponent(final BigDecimal cost, final Address from, final Address to) {
        return from.getStreet().equals(to.getStreet())
                ? BigDecimal.ZERO
                : cost.multiply(DISTANCE_COEFFICIENT).setScale(SCALE, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateVolumeCostComponent(final BigDecimal volume) {
        return volume.multiply(VOLUME_COEFFICIENT).setScale(SCALE, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateWeightCostComponent(final BigDecimal weight) {
        return weight.multiply(WEIGHT_COEFFICIENT).setScale(SCALE, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateFragileCostComponent(final BigDecimal cost, final boolean isFragile) {
        return isFragile
                ? cost.multiply(FRAGILE_COEFFICIENT).setScale(SCALE, RoundingMode.HALF_UP)
                : BigDecimal.ZERO;
    }

    private BigDecimal calculateWarehouseCostComponent(final BigDecimal cost, final Address warehouse) {
        Objects.requireNonNull(warehouse);
        BigDecimal coefficient = warehouse.getCountry().contains("ADDRESS_2")
                ? ADDRESS_2_COEFFICIENT
                : ADDRESS_1_COEFFICIENT;
        return cost.multiply(coefficient).setScale(SCALE, RoundingMode.HALF_UP);
    }

}
