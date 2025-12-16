package ru.yandex.practicum.handler.hub;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
public class HubEventHandlers {
    final Map<String, HubEventHandler> handlers;

    public HubEventHandlers(Set<HubEventHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toMap(HubEventHandler::getType, h -> h));
    }
}
