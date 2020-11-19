package stream.api;

import common.test.tool.annotation.Difficult;
import common.test.tool.annotation.Easy;
import common.test.tool.dataset.ClassicOnlineStore;
import common.test.tool.entity.Customer;
import common.test.tool.entity.Item;
import common.test.tool.util.CollectorImpl;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class Exercise9Test extends ClassicOnlineStore {

    @Easy @Test
    public void simplestStringJoin() {
        List<Customer> customerList = this.mall.getCustomerList();

        /**
         * Implement a {@link Collector} which can create a String with comma separated names shown in the assertion.
         * The collector will be used by serial stream.
         */
        Supplier<List<String>> supplier = ArrayList::new;
        BiConsumer<List<String>, String> accumulator = List::add;
        BinaryOperator<List<String>> combiner = (strings, strings2) ->
                Stream.concat(strings.stream(), strings2.stream()).collect(toList());
        Function<List<String>, String> finisher = strings -> String.join(",", strings);

        Collector<String, ?, String> toCsv =
            new CollectorImpl<>(supplier, accumulator, combiner, finisher, Collections.emptySet());
        String nameAsCsv = customerList.stream().map(Customer::getName).collect(toCsv);
        assertThat(nameAsCsv, is("Joe,Steven,Patrick,Diana,Chris,Kathy,Alice,Andrew,Martin,Amy"));
    }

    @Difficult @Test
    public void mapKeyedByItems() {
        List<Customer> customerList = this.mall.getCustomerList();

        /**
         * Implement a {@link Collector} which can create a {@link Map} with keys as item and
         * values as {@link Set} of customers who are wanting to buy that item.
         * The collector will be used by parallel stream.
         */
        BiFunction<Set<String>, Set<String>, Set<String>> mergeSets = (set1, set2) ->
                Stream.concat(set1.stream(), set2.stream()).collect(toSet());

        Supplier<Map<String, Set<String>>> supplier = ConcurrentHashMap::new;
        BiConsumer<Map<String, Set<String>>, Customer> accumulator = (itemsOfCustomers, customer) ->
                customer.getWantToBuy().forEach(item ->
                        itemsOfCustomers.merge(item.getName(),
                                newSetWithCustomer(customer),
                                mergeSets));
        BinaryOperator<Map<String, Set<String>>> combiner = (items1, items2) ->
                Stream.concat(items1.entrySet().stream(), items2.entrySet().stream())
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, mergeSets::apply));
        Function<Map<String, Set<String>>, Map<String, Set<String>>> finisher = items -> items;

        Collector<Customer, ?, Map<String, Set<String>>> toItemAsKey =
            new CollectorImpl<>(supplier, accumulator, combiner, finisher, EnumSet.of(
                Collector.Characteristics.CONCURRENT,
                Collector.Characteristics.IDENTITY_FINISH));
        Map<String, Set<String>> itemMap = customerList.stream().parallel().collect(toItemAsKey);
        assertThat(itemMap.get("plane"), containsInAnyOrder("Chris"));
        assertThat(itemMap.get("onion"), containsInAnyOrder("Patrick", "Amy"));
        assertThat(itemMap.get("ice cream"), containsInAnyOrder("Patrick", "Steven"));
        assertThat(itemMap.get("earphone"), containsInAnyOrder("Steven"));
        assertThat(itemMap.get("plate"), containsInAnyOrder("Joe", "Martin"));
        assertThat(itemMap.get("fork"), containsInAnyOrder("Joe", "Martin"));
        assertThat(itemMap.get("cable"), containsInAnyOrder("Diana", "Steven"));
        assertThat(itemMap.get("desk"), containsInAnyOrder("Alice"));
    }

    private Set<String> newSetWithCustomer(Customer customer) {
        Set<String> result = new HashSet<>();
        result.add(customer.getName());
        return result;
    }

    @Difficult @Test
    public void bitList2BitString() {
        String bitList = "22-24,9,42-44,11,4,46,14-17,5,2,38-40,33,50,48";

        /**
         * Create a {@link String} of "n"th bit ON.
         * for example
         * "3" will be "001"
         * "1,3,5" will be "10101"
         * "1-3" will be "111"
         * "7,1-3,5" will be "1110101"
         */
        Supplier<List<Integer>> supplier = ArrayList::new;
        BiConsumer<List<Integer>, String> accumulator = (bits, str) -> bits.addAll(processNth(str));
        BinaryOperator<List<Integer>> combiner = (bits1, bits2) -> Stream.concat(bits1.stream(), bits2.stream()).collect(toList());
        Function<List<Integer>, String> finisher = this::createBitStringFromPositions;

        Collector<String, ?, String> toBitString = new CollectorImpl<>(supplier, accumulator, combiner, finisher, Collections.emptySet());

        String bitString = Arrays.stream(bitList.split(",")).collect(toBitString);
        assertThat(bitString, is("01011000101001111000011100000000100001110111010101")

        );
    }

    private String createBitStringFromPositions(List<Integer> bitPositions) {
        StringBuilder bitStream = new StringBuilder();
        IntStream.rangeClosed(1, bitPositions.stream().max(Integer::compareTo).get())
                .forEach(i -> bitStream.append(bitPositions.contains(i) ? 1 : 0));
        return bitStream.toString();
    }

    private List<Integer> processNth(String bitPosition) {
        List<Integer> result = new ArrayList<>();
        List<Integer> bitPositions = Arrays.stream(bitPosition.split("-")).map(Integer::valueOf).collect(toList());
        result.add(bitPositions.get(0));
        if (bitPositions.size() == 2) {
            IntStream.rangeClosed(bitPositions.get(0)+1, bitPositions.get(1)).forEach(result::add);
        }
        return result;
    }
}
