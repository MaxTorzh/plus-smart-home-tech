package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;
import ru.yandex.practicum.model.WarehouseProduct;
import ru.yandex.practicum.dto.NewProductInWarehouse;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface WarehouseMapper {

    WarehouseProduct toWarehouse(NewProductInWarehouse request);
}
