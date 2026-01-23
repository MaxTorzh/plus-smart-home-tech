package ru.yandex.practicum.commerce.warehouse.util;

import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.warehouse.model.Dimension;
import ru.yandex.practicum.commerce.warehouse.model.Product;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;

@Component
@Slf4j
public class ProductDimensionCalculator {

    public BookedProductsDto calculateDimension(
            final Map<UUID, Long> products,
            final Map<UUID, Product> stock
    ) {
        log.debug("Calculating overall dimension values for products: {}. Dimension Specifications from:{}",
                products, stock);

        double totalWeight = 0;
        double totalVolume = 0;
        boolean fragile = false;

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            long quantity = entry.getValue();
            Product product = stock.get(productId);

            totalWeight += product.getWeight() * quantity;
            Dimension dimension = product.getDimension();
            totalVolume += dimension.getWidth() * dimension.getHeight() * dimension.getDepth() * quantity;

            if (product.isFragile()) {
                fragile = true;
            }
        }
        log.debug("Total weight: {}, total volume: {}, fragile: {}", totalWeight, totalVolume, fragile);

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(fragile)
                .build();
    }
}
