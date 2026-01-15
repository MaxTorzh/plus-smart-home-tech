package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Entity representing a product in the warehouse.
 */
@Entity
@Table(schema = "warehouse", name = "products")
@EqualsAndHashCode(of = "productId")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {

    @Id
    @Column(name = "product_id", nullable = false, updatable = false)
    private UUID productId;

    @Column(name = "fragile", nullable = false)
    private boolean fragile;

    @Embedded
    private Dimension dimension;

    @Column(name = "weight", nullable = false)
    private double weight;

    @Column(name = "quantity", nullable = false)
    private long quantity;
}
