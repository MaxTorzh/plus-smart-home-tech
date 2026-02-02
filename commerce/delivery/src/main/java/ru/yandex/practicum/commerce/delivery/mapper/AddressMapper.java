package ru.yandex.practicum.commerce.delivery.mapper;

import java.util.Objects;
import lombok.experimental.UtilityClass;
import ru.yandex.practicum.commerce.delivery.model.Address;
import ru.yandex.practicum.dto.warehouse.AddressDto;

@UtilityClass
public class AddressMapper {

    public Address toEntity(final AddressDto addressDto) {
        Objects.requireNonNull(addressDto);
        return Address.builder()
                .country(addressDto.getCountry())
                .city(addressDto.getCity())
                .street(addressDto.getStreet())
                .house(addressDto.getHouse())
                .flat(addressDto.getFlat())
                .build();
    }

    public static AddressDto toDto(final Address address) {
        Objects.requireNonNull(address);
        return AddressDto.builder()
                .country(address.getCountry())
                .city(address.getCity())
                .street(address.getStreet())
                .house(address.getHouse())
                .flat(address.getFlat())
                .build();
    }
}