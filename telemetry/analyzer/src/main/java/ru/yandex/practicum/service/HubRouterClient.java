package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.mapper.Mapper;
import ru.yandex.practicum.model.Action;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class HubRouterClient {

    @GrpcClient("hub-router")
    HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;
    final Mapper mapper;

    public void sendRequest(Action action) {
        try {
            DeviceActionRequest actionRequest = mapper.toActionRequest(action);
            hubRouter.handleDeviceAction(actionRequest);
        } catch (Exception e) {
            log.error("Возникла ошибка при отправке запроса", e);
        }
    }
}
