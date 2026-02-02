package ru.yandex.practicum.commerce.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableFeignClients(basePackages = "ru.yandex.practicum.feign")
@ComponentScan(basePackages = {"ru.yandex.practicum.commerce.payment", "ru.yandex.practicum.feign"})
public class PaymentApp {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApp.class, args);
    }
}
