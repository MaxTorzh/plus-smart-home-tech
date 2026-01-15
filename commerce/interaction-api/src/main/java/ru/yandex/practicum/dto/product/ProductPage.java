package ru.yandex.practicum.dto.product;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Representation of a product page.
 */
@Data
@NoArgsConstructor
public class ProductPage implements Page<ProductDto> {

    @JsonProperty("content")
    private List<ProductDto> content;

    @JsonProperty("totalElements")
    private long totalElements;

    @JsonProperty("totalPages")
    private int totalPages;

    @JsonProperty("number")
    private int number;

    @JsonProperty("size")
    private int size;

    @JsonProperty("sort")
    private Sort sort;

    private Pageable pageable;

    public ProductPage(final Page<ProductDto> page) {
        this.content = page.getContent();
        this.totalElements = page.getTotalElements();
        this.totalPages = page.getTotalPages();
        this.number = page.getNumber();
        this.size = page.getSize();
        this.pageable = page.getPageable();
        this.sort = pageable.getSort();
    }

    public <U> ProductPage(List<U> mappedContent, Pageable pageable, long totalElements) {
    }

    @Override
    public int getTotalPages() {
        return totalPages;
    }

    @Override
    public long getTotalElements() {
        return totalElements;
    }

    @Override
    public int getNumber() {
        return number;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int getNumberOfElements() {
        return content.size();
    }

    @Override
    @NonNull
    public List<ProductDto> getContent() {
        return content;
    }

    @Override
    public boolean hasContent() {
        return !content.isEmpty();
    }

    @Override
    @NonNull
    public Sort getSort() {
        return sort;
    }

    @Override
    public boolean isFirst() {
        return !pageable.hasPrevious();
    }

    @Override
    public boolean isLast() {
        return !hasNext();
    }

    @Override
    public boolean hasNext() {
        return number + 1 < totalPages;
    }

    @Override
    public boolean hasPrevious() {
        return number > 0;
    }

    @Override
    @NonNull
    public Pageable getPageable() {
        return pageable;
    }

    @Override
    @NonNull
    public Pageable nextPageable() {
        return hasNext() ? pageable.next() : Pageable.unpaged();
    }

    @Override
    @NonNull
    public Pageable previousPageable() {
        return hasPrevious() ? pageable.previousOrFirst() : Pageable.unpaged();
    }

    @Override
    @NonNull
    public <U> Page<U> map(@NonNull Function<? super ProductDto, ? extends U> converter) {
        List<U> mappedContent = content.stream().map(converter).collect(Collectors.toList());
        return (Page<U>) new ProductPage(mappedContent, pageable, totalElements);
    }

    @Override
    @NonNull
    public Iterator<ProductDto> iterator() {
        return content.iterator();
    }
}
