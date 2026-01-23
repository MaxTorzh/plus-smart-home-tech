package ru.yandex.practicum.commerce.warehouse.service;

import java.security.SecureRandom;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.warehouse.AddressDto;

/**
 * Provides: Актуальный адрес склада
 */
@Component
@Slf4j
public class WarehouseAddressServiceImpl implements WarehouseAddressService {

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};

    private static final String CURRENT_ADDRESS =
            ADDRESSES[Random.from(new SecureRandom()).nextInt(0, 1)];

    @Override
    public AddressDto getAddress() {
        AddressDto address = AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
        log.debug("Generated current address for the warehouse: {}", address);
        return address;
    }
}
