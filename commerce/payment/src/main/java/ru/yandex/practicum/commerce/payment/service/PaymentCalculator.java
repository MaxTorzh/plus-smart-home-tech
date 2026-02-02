package ru.yandex.practicum.commerce.payment.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.feign.ShoppingStoreOperations;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentCalculator {

    private static final BigDecimal FEE_RATE = BigDecimal.valueOf(0.1);
    private static final int SCALE = 2;
    private final ShoppingStoreOperations storeClient;


    public BigDecimal calculateProductCost(final Map<UUID, Long> products) {
        return products.entrySet().stream()
                .map(entry -> {
                    UUID productId = entry.getKey();
                    Long quantity = entry.getValue();
                    ProductDto product = storeClient.getProductById(productId);
                    return product.getPrice().multiply(BigDecimal.valueOf(quantity));
                })
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(SCALE, RoundingMode.HALF_UP);
    }

    public BigDecimal calculateTotal(final BigDecimal deliveryPrice, final BigDecimal productPrice) {
        final BigDecimal fee = productPrice.multiply(FEE_RATE).setScale(SCALE, RoundingMode.HALF_UP);
        return productPrice.add(fee).add(deliveryPrice);
    }

}

