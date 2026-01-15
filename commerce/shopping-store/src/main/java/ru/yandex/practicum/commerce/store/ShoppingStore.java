package ru.yandex.practicum.commerce.store;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {
        "ru.yandex.practicum.commerce.store",
        "ru.yandex.practicum.exception.handler"
})
public class ShoppingStore {

    public static void main(String[] args) {
        SpringApplication.run(ShoppingStore.class, args);
    }
}