package ru.yandex.practicum.dto;

import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Pageable {
    @Min(value = 0, message = "page should not be less than 1")
    private int page;

    @Min(value = 1, message = "size should not be less than 1")
    private int size;

    private List<String> sort = new ArrayList<>();
}
