package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.Column;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO represents product dimensions.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Dimension {

    @Column(name = "depth")
    private Double depth;

    @Column(name = "height")
    private Double height;

    @Column(name = "width")
    private Double width;
}
