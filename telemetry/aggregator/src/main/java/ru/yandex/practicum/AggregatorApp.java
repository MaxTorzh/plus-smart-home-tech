package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.service.AggregationStarter;

/**
 * AggregatorApp is the main Spring Boot application class for the telemetry aggregator service.
 *
 * This application consumes sensor events from Kafka, processes and aggregates them into
 * snapshots of sensor states, and produces these snapshots back to Kafka. It serves as
 * the entry point for the aggregator microservice in the smart home system.
 *
 * The application uses Spring Boot auto-configuration and component scanning to wire up
 * all necessary services, including Kafka consumers and producers, and sensor state management.
 *
 * After starting the Spring context, it retrieves the AggregationStarter bean and begins
 * the continuous process of consuming sensor events and producing aggregated snapshots.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AggregatorApp {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApp.class, args);

        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        aggregator.start();
    }
}
