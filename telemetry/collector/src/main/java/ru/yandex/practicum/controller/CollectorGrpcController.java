package ru.yandex.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.handler.sensor.SensorEventHandler;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CollectorGrpcController is a gRPC service that handles incoming telemetry events
 * from smart home devices and hubs. It receives both hub events and sensor events,
 * routes them to appropriate handlers based on their type, and returns empty responses.
 *
 * This controller implements the CollectorControllerGrpc service interface and
 * serves as the entry point for external systems to send telemetry data to the system.
 */
@GrpcService
public class CollectorGrpcController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private final Map<HubEventProto.PayloadCase, HubEventHandler> hubEventHandlers;
    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorEventHandlers;

    /**
     * Constructs a CollectorGrpcController with the provided hub and sensor event handlers.
     *
     * @param hubEventHandlers set of hub event handlers to register
     * @param sensorEventHandlers set of sensor event handlers to register
     */
    public CollectorGrpcController(Set<HubEventHandler> hubEventHandlers,
                                   Set<SensorEventHandler> sensorEventHandlers) {
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
    }

    /**
     * Handles incoming hub events by routing them to the appropriate handler.
     *
     * @param request the hub event to process
     * @param responseObserver the response observer to send back results
     */
    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            if (hubEventHandlers.containsKey(request.getPayloadCase())) {
                hubEventHandlers.get(request.getPayloadCase()).handle(request);
            } else {
                throw new IllegalArgumentException("Handler not found for event " + request.getPayloadCase());
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }

    /**
     * Handles incoming sensor events by routing them to the appropriate handler.
     *
     * @param request the sensor event to process
     * @param responseObserver the response observer to send back results
     */
    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            if (sensorEventHandlers.containsKey(request.getPayloadCase())) {
                sensorEventHandlers.get(request.getPayloadCase()).handle(request);
            } else {
                throw new IllegalArgumentException("Handler not found for event " + request.getPayloadCase());
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }
}
