package ru.yandex.practicum.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Embeddable
public class Size {
    @Column(name = "width", nullable = false)
    private double width;
    @Column(name = "height", nullable = false)
    private double height;
    @Column(name = "depth", nullable = false)
    private double depth;
}