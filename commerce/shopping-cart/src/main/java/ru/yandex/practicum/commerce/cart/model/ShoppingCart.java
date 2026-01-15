package ru.yandex.practicum.commerce.cart.model;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Table;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a shopping cart in the online store.
 */
@Entity
@Table(name = "shopping_carts", schema = "cart")
@Getter
@Setter
@EqualsAndHashCode(of = "cartId")
@ToString(exclude = "products")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCart {

    @Id
    @Column(name = "cart_id", updatable = false, nullable = false)
    private UUID cartId;

    @Column(name = "username", nullable = false)
    private String username;

    @Column(name = "cart_state", nullable = false)
    @Enumerated(EnumType.STRING)
    private ShoppingCartState cartState;

    @ElementCollection
    @CollectionTable(name = "shopping_cart_products", schema = "cart",
            joinColumns = @JoinColumn(name = "cart_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products = new HashMap<>();
}

