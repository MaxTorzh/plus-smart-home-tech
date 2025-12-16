package ru.yandex.practicum;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.service.HubEventProcessor;
import ru.yandex.practicum.service.SnapshotProcessor;

/**
 * AnalyzerApp is the main Spring Boot application class for the telemetry analyzer service.
 *
 * This application consumes hub events and sensor snapshots from Kafka, processes them,
 * and triggers appropriate actions based on defined scenarios. It serves as the entry point
 * for the analyzer microservice in the smart home system.
 *
 * The application uses Spring Boot auto-configuration and component scanning to wire up
 * all necessary services, including Kafka consumers, database repositories, and gRPC clients.
 *
 * After starting the Spring context, it retrieves the HubEventProcessor and SnapshotProcessor
 * beans and starts them in separate threads to handle hub events and sensor snapshots concurrently.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AnalyzerApp {
    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(AnalyzerApp.class, args);

        final HubEventProcessor hubEventProcessor =
                context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor =
                context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        snapshotProcessor.start();
    }
}
