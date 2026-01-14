package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.mapper.WarehouseMapper;
import ru.yandex.practicum.model.WarehouseProduct;
import ru.yandex.practicum.repository.WarehouseRepository;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.exception.ConditionsNotMetException;
import ru.yandex.practicum.exception.NotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class WarehouseServiceImpl implements WarehouseService {
    private final WarehouseRepository warehouseRepository;
    private final WarehouseMapper warehouseMapper;

    @Transactional
    @Override
    public void createProductInWarehouse(NewProductInWarehouse request) {
        Optional<WarehouseProduct> product = findProduct(request.getProductId());
        if (product.isPresent())
            throw new ConditionsNotMetException("This item already in database");
        warehouseRepository.save(warehouseMapper.toWarehouse(request));
    }

    @Override
    public ReserveProductsDto checkCountProducts(CartDto cartDto) {
        Map<String, Long> products = cartDto.getProducts();
        List<WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(products.keySet());
        warehouseProducts.forEach(warehouseProduct -> {
            if (warehouseProduct.getQuantity() < products.get(warehouseProduct.getProductId()))
                throw new ConditionsNotMetException("Not enough items in the warehouse");
        });

        double deliveryWeight = warehouseProducts.stream()
                .map(WarehouseProduct::getWeight)
                .mapToDouble(Double::doubleValue)
                .sum();

        double deliveryVolume = warehouseProducts.stream()
                .map(warehouseProduct -> warehouseProduct.getDimension().getDepth()
                        * warehouseProduct.getDimension().getHeight() * warehouseProduct.getDimension().getWidth())
                .mapToDouble(Double::doubleValue)
                .sum();

        boolean fragile = warehouseProducts.stream()
                .anyMatch(WarehouseProduct::isFragile);
        return ReserveProductsDto.builder()
                .deliveryWeight(deliveryWeight)
                .deliveryVolume(deliveryVolume)
                .fragile(fragile)
                .build();
    }

    @Transactional
    @Override
    public void addProductInWarehouse(AddProductInWarehouse request) {
        Optional<WarehouseProduct> product = findProduct(request.getProductId());
        if (product.isEmpty())
            throw new NotFoundException("This item is not found");
        WarehouseProduct pr = product.get();
        pr.setQuantity(pr.getQuantity() + request.getQuantity());
        warehouseRepository.save(pr);
    }

    @Override
    public AddressWarehouseDto getAddressWarehouse() {
        return AddressWarehouseDto.builder()
                .country("Россия")
                .city("Липецк")
                .street("Ленина")
                .house("1")
                .flat("5")
                .build();
    }

    private Optional<WarehouseProduct> findProduct(String productId) {

        return warehouseRepository.findById(productId);
    }
}
