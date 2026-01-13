package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.ShoppingCart;
import ru.yandex.practicum.dto.CartDto;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface CartMapper {
    CartDto toShoppingCartDto(ShoppingCart shoppingCart);
}
