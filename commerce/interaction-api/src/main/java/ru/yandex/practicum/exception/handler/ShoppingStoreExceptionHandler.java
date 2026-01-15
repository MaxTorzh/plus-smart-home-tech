package ru.yandex.practicum.exception.handler;

import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(basePackages = "ru.yandex.practicum.commerce.store")
public class ShoppingStoreExceptionHandler extends GlobalExceptionHandler {
}
