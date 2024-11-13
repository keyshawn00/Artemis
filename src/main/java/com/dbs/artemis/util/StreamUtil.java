package com.dbs.artemis.util;

import lombok.experimental.UtilityClass;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public class StreamUtil {

    public static <T> Stream<T> of(Collection<T> collection) {
        return Optional.ofNullable(collection).stream().flatMap(Collection::stream);
    }

    public static <T> List<T> filterToList(Collection<T> collection, Predicate<T> predicate) {
        return of(collection).filter(predicate).collect(Collectors.toList());
    }

    public static <T, R> List<R> toList(Collection<T> collection, Function<? super T, ? extends R> function) {
        return of(collection).map(function).collect(Collectors.toList());
    }

    public static <T> T findFirst(Collection<T> collection, Predicate<T> predicate, T or) {
        return of(collection).filter(predicate).findFirst().orElse(or);
    }

    public static <T,K, V> Map<K, V> toMap(Collection<T> collection, Function<T, K> key, Function<T, V> value) {
        return of(collection).collect(Collectors.toMap(key, value, (k1, k2) -> k1));
    }

    public static <K, T> Map<K, List<T>> group(Collection<T> collection, Function<T, K> key) {
        return of(collection).collect(Collectors.groupingBy(key));
    }

}
