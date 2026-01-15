package ru.yandex.practicum.commerce.store.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import ru.yandex.practicum.dto.product.QuantityState;
import ru.yandex.practicum.dto.convert.StringToEnumConverter;

@Configuration
public class ShoppingStoreWebConfig implements WebMvcConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new StringToEnumConverter<>(QuantityState.class));
    }
}
