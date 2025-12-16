package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * CollectorApp is the main Spring Boot application class for the telemetry collector service.
 *
 * This application serves as a gRPC server that collects telemetry events from smart home
 * devices and hubs, processes them, and forwards them to Kafka topics for further analysis.
 * It handles both hub-level events (like device additions/removals and scenario changes)
 * and sensor-level events (like temperature, motion, and light readings).
 *
 * The application uses Spring Boot auto-configuration and component scanning to wire up
 * all necessary services, including gRPC controllers, Kafka producers, and event mappers.
 */
@SpringBootApplication
public class CollectorApp {
    public static void main(String[] args) {
        SpringApplication.run(CollectorApp.class, args);
    }
}
