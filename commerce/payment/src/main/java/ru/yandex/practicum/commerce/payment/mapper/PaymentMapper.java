package ru.yandex.practicum.commerce.payment.mapper;

import java.util.Objects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.dto.payment.PaymentDto;

@UtilityClass
@Slf4j
public class PaymentMapper {


    public static PaymentDto toDto(final Payment payment) {
        Objects.requireNonNull(payment);
        return PaymentDto.builder()
                .paymentId(payment.getPaymentId())
                .totalPayment(payment.getTotalPayment())
                .deliveryTotal(payment.getDeliveryTotal())
                .feeTotal(payment.getFeeTotal())
                .build();
    }
}