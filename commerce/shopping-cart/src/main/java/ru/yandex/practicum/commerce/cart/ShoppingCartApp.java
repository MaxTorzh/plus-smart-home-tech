package ru.yandex.practicum.commerce.cart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableFeignClients(basePackages = "ru.yandex.practicum.feign")
@ComponentScan(basePackages = {
        "ru.yandex.practicum.commerce.cart",
        "ru.yandex.practicum.exception.handler"
})
public class ShoppingCartApp {

    public static void main(String[] args) {
        SpringApplication.run(ShoppingCartApp.class,args);

    }
}
