package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.service.handler.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.handler.sensor.SensorEventHandler;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link  EventController} is a {@code gRPC} service for processing sensor and hub events received from a {@code Hub Router}.
 * <p>
 * It implements the gRPC API defined in {@link CollectorControllerGrpc.CollectorControllerImplBase},
 * providing two RPC methods: {@code collectSensorEvent} and {@code collectHubEvent}.
 * <p>
 * This service is responsible for:
 * <ul>
 *   <li>Receiving sensor events via the {@code collectSensorEvent} method.</li>
 *   <li>Receiving hub events via the {@code collectHubEvent} method.</li>
 *   <li>Dispatching events to the appropriate handler based on the event's payload type.</li>
 *   <li>Handling errors and returning appropriate responses to the gRPC client.</li>
 * </ul>
 */
@GrpcService
@Slf4j
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorEventHandlers;

    private final Map<HubEventProto.PayloadCase, HubEventHandler> hubEventHandlers;

    public EventController(final Set<SensorEventHandler> sensorEventHandlers,
                           final Set<HubEventHandler> hubEventHandlers) {
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
        log.debug("Initialized EventController with {} event handlers and {} hub handlers.",
                sensorEventHandlers.size(), hubEventHandlers.size());
    }


    /**
     *  Processes sensor events received from a gRPC client.
     *
     * @param request          The sensor event received.
     * @param responseObserver The response observer used to send a response back to the client.
     */
    @Override
    public void collectSensorEvent(final SensorEventProto request,
                                   final StreamObserver<Empty> responseObserver) {
        log.info("Received sensor event: id={}, payloadType={}", request.getId(),
                request.getPayloadCase());
        try {
            if (sensorEventHandlers.containsKey(request.getPayloadCase())) {
                sensorEventHandlers.get(request.getPayloadCase()).handle(request);
            } else {
                log.error("No handler found for event type: {}", request.getPayloadCase());
                throw new IllegalArgumentException(
                        "Cannot find a handler for event type " + request.getPayloadCase());
            }
            log.info("Successfully processed event: id={}", request.getId());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while processing event: id={}, error={}", request.getId(), e.getMessage(),
                    e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }

    /**
     * Processes hub events received from a gRPC client.
     *
     * @param request          The hub event received.
     * @param responseObserver The response observer used to send a response.
     */
    @Override
    public void collectHubEvent(final HubEventProto request,
                                final StreamObserver<Empty> responseObserver) {
        log.info("Received hub event of type: {}.", request.getPayloadCase());
        try {
            if (hubEventHandlers.containsKey(request.getPayloadCase())) {
                hubEventHandlers.get(request.getPayloadCase()).handle(request);
            } else {
                log.error("No handler found for the hub event type: {}", request.getPayloadCase());
                throw new IllegalArgumentException(
                        "Cannot find a handler for the hub event type " + request.getPayloadCase());
            }
            log.info("Successfully processed hub event: id={}", request.getHubId());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while processing hub event: hub_id={}, error={}", request.getHubId(), e.getMessage(),
                    e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }
}
