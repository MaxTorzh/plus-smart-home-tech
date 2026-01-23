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
import ru.yandex.practicum.commerce.warehouse.model.Booking;
import ru.yandex.practicum.commerce.warehouse.model.Product;
import ru.yandex.practicum.commerce.warehouse.repository.BookingRepository;
import ru.yandex.practicum.commerce.warehouse.repository.ProductRepository;
import ru.yandex.practicum.commerce.warehouse.util.ProductDimensionCalculator;
import ru.yandex.practicum.commerce.warehouse.util.UuidGeneratorImpl;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;
import ru.yandex.practicum.exception.NoBookingFoundException;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseServiceImpl implements WarehouseService {

    private final ProductRepository productRepository;
    private final BookingRepository bookingRepository;
    private final WarehouseAddressService addressService;
    private final ProductDimensionCalculator dimensionCalculator;
    private final UuidGeneratorImpl uuidGenerator;

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
        final Map<UUID, Product> stockProducts = getProductsStock(shoppingCart.getProducts().keySet());

        return validateAvailabilityAndCalculateDimensions(shoppingCart.getProducts(),stockProducts);
    }

    @Transactional
    @Override
    public void increaseProductQuantity(final AddProductToWarehouseRequest request) {
        log.debug("Increasing quantity of the product: {} by: {}.",
                request.getProductId(), request.getQuantity());

        final Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "productID: " + request.getProductId()));

        addToStock(product,request.getQuantity());

        final Product updatedProduct = productRepository.save(product);
        log.debug("Updated product: ID={}, quantity={}.", updatedProduct.getProductId(),
                updatedProduct.getQuantity());
    }


    @Transactional(readOnly = true)
    @Override
    public AddressDto getAddress() {
        return addressService.getAddress();
    }

    @Transactional
    @Override
    public void sendToDelivery(final ShippedToDeliveryRequest request) {
        log.debug("Marking order {} pas picked by delivery {}.",
                request.getOrderId(),request.getDeliveryId());

        final Booking booking = getBookingOrThrow(request.getOrderId());

        booking.setDeliveryId(request.getDeliveryId());
        bookingRepository.save(booking);
        log.info("Updated booking {} info, added delivery ID: {}.",booking.getBookingId(),booking.getDeliveryId());
    }

    @Transactional
    @Override
    public BookedProductsDto bookProducts(
            final AssemblyProductsForOrderRequest request
    ) {
        log.debug("Booking products {} for the order {}.", request.getProducts(), request.getOrderId());

        final Map<UUID, Long> requestedProducts = request.getProducts();
        final Map<UUID, Product> stockProducts = getProductsStock(requestedProducts.keySet());

        final BookedProductsDto dimensions = reserveProducts(requestedProducts,stockProducts);

        bookingRepository.save(buildBooking(request));
        return dimensions;
    }

    @Transactional
    @Override
    public void returnProducts(final Map<UUID, Long> products) {
        log.debug("Returning products {} to the warehouse by increasing stock.",products);

        final Map<UUID, Product> stock = getProductsStock(products.keySet());

        validateProductsExist(products.keySet(),stock.keySet());

        products.forEach((productId, quantity) ->
                addToStock(stock.get(productId), quantity));
        productRepository.saveAll(stock.values());
    }

    private Booking buildBooking(final AssemblyProductsForOrderRequest request) {
        log.debug("Creating Booking entity out of request {}.", request);
        return Booking.builder()
                .bookingId(uuidGenerator.generate())
                .orderId(request.getOrderId())
                .products(request.getProducts())
                .build();
    }

    private Map<UUID, Product> getProductsStock(final Set<UUID> productIds) {
        log.debug("Retrieving actual products stock, presented in the BD by their IDs: {}.", productIds);

        return productRepository.findAllById(productIds)
                .stream()
                .collect(Collectors.toMap(Product::getProductId, Function.identity()));
    }

    private Booking getBookingOrThrow(final UUID orderId) {
        log.debug("Retrieving booking info from the DB by order ID {}.", orderId);
        return bookingRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoBookingFoundException("Order ID: " + orderId));
    }

    private BookedProductsDto reserveProducts(final Map<UUID, Long> requestedProducts,
                                              final Map<UUID, Product> stockProducts) {
        final BookedProductsDto dimensions =
                validateAvailabilityAndCalculateDimensions( requestedProducts, stockProducts);

        requestedProducts.forEach((productId, quantity) ->
                reduceStock(stockProducts.get(productId), quantity));

        productRepository.saveAll(stockProducts.values());
        return dimensions;
    }

    private void reduceStock(final Product product, final Long quantity) {
        product.setQuantity(product.getQuantity() - quantity);
    }

    private void addToStock(Product product, Long quantity) {
        product.setQuantity(product.getQuantity() + quantity);
    }

    private BookedProductsDto validateAvailabilityAndCalculateDimensions(
            final Map<UUID, Long> requestedProducts,
            final Map<UUID, Product> stockProducts
    ) {
        log.debug("Checking desired products available in the warehouse: {}.", requestedProducts);

        validateProductsExist(requestedProducts.keySet(), stockProducts.keySet());
        validateProductQuantities(requestedProducts, stockProducts);

        return dimensionCalculator.calculateDimension(requestedProducts, stockProducts);
    }

    private void validateProductQuantities(final Map<UUID, Long> requestedProducts,
                                           final Map<UUID, Product> stockProducts) {
        Map<UUID, Long> stock = stockProducts.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getQuantity()
                ));

        log.debug(
                "Validating that all required products are available in the warehouse; required:{}, stock: {}.",
                requestedProducts, stock);

        final Set<UUID> shortProducts = stockProducts.entrySet().stream()
                .filter(entry -> entry.getValue().getQuantity() < requestedProducts.get(entry.getKey()))
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        if (!shortProducts.isEmpty()) {
            log.warn("Validating products quantity failed, warehouse is short in products: {} ",
                    shortProducts);
            throw new ProductInShoppingCartLowQuantityInWarehouse(
                    "Shortage in products: " + shortProducts);
        }
    }

    private void validateProductsExist(final Set<UUID> requested, final Set<UUID> stock) {
        log.debug(
                "Validating that all required products are stored in the warehouse; required: {}, stored: {}",
                requested, stock);

        final Set<UUID> missingProducts = requested.stream()
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
