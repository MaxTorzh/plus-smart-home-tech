package ru.yandex.practicum.commerce.payment.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.yandex.practicum.dto.payment.PaymentStatus;

/**
 * Entity representing a payment data in the online store.
 */

@Entity
@Table(name = "payments", schema = "payment")
@Getter
@Setter
@EqualsAndHashCode(of = "paymentId")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    @Id
    @Column(name = "payment_id", updatable = false, nullable = false)
    private UUID paymentId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "delivery_total")
    private BigDecimal deliveryTotal;

    @Column(name = "total_payment")
    private BigDecimal totalPayment;

    @Column(name = "fee_total")
    private BigDecimal feeTotal;

    @Column(name = "status", nullable = false)
    @Enumerated(value = EnumType.STRING)
    private PaymentStatus status;

}