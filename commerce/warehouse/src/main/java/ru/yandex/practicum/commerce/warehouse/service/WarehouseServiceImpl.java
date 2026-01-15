package ru.yandex.practicum.commerce.warehouse.service;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.warehouse.mapper.ProductMapper;
import ru.yandex.practicum.commerce.warehouse.model.Dimension;
import ru.yandex.practicum.commerce.warehouse.model.Product;
import ru.yandex.practicum.commerce.warehouse.repository.ProductRepository;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseServiceImpl implements WarehouseService {

    private final ProductRepository productRepository;
    private final WarehouseAddressService addressService;

    @Transactional
    @Override
    public void addNewProduct(final NewProductInWarehouseRequest request) {
        log.debug("Adding new product to the warehouse: {}", request);
        validateProductIsNew(request.getProductId());
        final Product product = ProductMapper.toEntity(request);
        Product savedProduct = productRepository.save(product);
        log.debug("Added product tho the warehouse: ID:{}, quantity:{} ",
                savedProduct.getProductId(), savedProduct.getQuantity());
    }

    @Transactional
    @Override
    public BookedProductsDto checkStock(final ShoppingCartDto shoppingCart) {
        log.debug("Checking stock for the shopping cart {}.", shoppingCart);
        validateCartNotEmpty(shoppingCart);

        final Map<UUID, Long> cartProducts = shoppingCart.getProducts();

        final Map<UUID, Product> stockProducts = productRepository.findAllById(cartProducts.keySet())
                .stream()
                .collect(Collectors.toMap(Product::getProductId, Function.identity()));
        validateProductsExistedInWarehouse(cartProducts.keySet(), stockProducts.keySet());
        validateProductQuantity(cartProducts, stockProducts);
        return calculateBookedProducts(cartProducts, stockProducts);
    }

    @Transactional
    @Override
    public void increaseProductQuantity(final AddProductToWarehouseRequest request) {
        log.debug("Increasing quantity of the product: {} by: {}.",
                request.getProductId(), request.getQuantity());

        final Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "productID: " + request.getProductId()));
        product.setQuantity(product.getQuantity() + request.getQuantity());

        final Product updatedProduct = productRepository.save(product);
        log.debug("Updated product: ID={}, quantity={}.", updatedProduct.getProductId(),
                updatedProduct.getQuantity());
    }


    @Transactional(readOnly = true)
    @Override
    public AddressDto getAddress() {
        return addressService.getAddress();
    }

    private BookedProductsDto calculateBookedProducts(final Map<UUID, Long> cartProducts,
                                                      final Map<UUID, Product> stockProducts) {
        log.debug(
                "Calculating overall dimension specs for products in the cart: {}. Dimension Specifications from:{}",
                cartProducts, stockProducts);
        double totalWeight = 0;
        double totalVolume = 0;
        boolean fragile = false;

        for (Map.Entry<UUID, Long> entry : cartProducts.entrySet()) {
            UUID productId = entry.getKey();
            long quantity = entry.getValue();
            Product product = stockProducts.get(productId);
            totalWeight += product.getWeight() * quantity;
            Dimension dim = product.getDimension();
            totalVolume += dim.getWidth() * dim.getHeight() * dim.getDepth() * quantity;

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

    private void validateProductQuantity(final Map<UUID, Long> cartProducts,
                                         final Map<UUID, Product> stockProducts) {

        Map<UUID, Long> stock = stockProducts.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getQuantity()
                ));

        log.debug(
                "Validating that all required products are available in the warehouse; required:{}, stock: {}.",
                cartProducts, stock);

        final Set<UUID> shortProducts = stockProducts.entrySet().stream()
                .filter(entry -> entry.getValue().getQuantity() < cartProducts.get(entry.getKey()))
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        if (!shortProducts.isEmpty()) {
            log.warn("Validating products quantity failed, warehouse is short in products: {} ",
                    shortProducts);
            throw new ProductInShoppingCartLowQuantityInWarehouse(
                    "Shortage in products: " + shortProducts);
        }
    }

    private void validateProductsExistedInWarehouse(final Set<UUID> required, final Set<UUID> stock) {
        log.debug(
                "Validating that all required products are stored in the warehouse; required: {}, stored: {}",
                required, stock);

        final Set<UUID> missingProducts = required.stream()
                .filter(uuid -> !stock.contains(uuid))
                .collect(Collectors.toSet());

        if (!missingProducts.isEmpty()) {
            log.warn("Validating item existence in the warehouse failed, Missing products: {}.",
                    missingProducts);
            throw new NoSpecifiedProductInWarehouseException("Missing products: " + missingProducts);
        }
        log.debug("Success: all required products are registered in the warehouse.");
    }

    private void validateProductIsNew(final UUID productId) {
        log.debug("Validating product {} does not exist in the warehouse yet.", productId);
        if (productRepository.existsById(productId)) {
            throw new SpecifiedProductAlreadyInWarehouseException("productID: " + productId);
        }
    }

    private void validateCartNotEmpty(final ShoppingCartDto shoppingCart) {
        log.debug("Validating shopping cart contains product to check: {}", shoppingCart);
        if (shoppingCart == null ||
                shoppingCart.getProducts() == null ||
                shoppingCart.getProducts().isEmpty()
        ) {
            throw new NoProductsInShoppingCartException("Shopping cart: " + shoppingCart);
        }
    }
}
