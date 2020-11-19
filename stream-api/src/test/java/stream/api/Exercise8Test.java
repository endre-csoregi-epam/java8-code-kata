package stream.api;

import common.test.tool.annotation.Difficult;
import common.test.tool.dataset.ClassicOnlineStore;
import common.test.tool.entity.Customer;
import common.test.tool.entity.Item;
import common.test.tool.entity.Shop;

import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

public class Exercise8Test extends ClassicOnlineStore {

    @Difficult @Test
    public void itemsNotOnSale() {
        Stream<Customer> customerStream = this.mall.getCustomerList().stream();
        Stream<Shop> shopStream = this.mall.getShopList().stream();

        /**
         * Create a set of item names that are in {@link Customer.wantToBuy} but not on sale in any shop.
         */
        List<String> itemListOnSale = shopStream
                .map(Shop::getItemList)
                .flatMap(Collection::stream)
                .map(Item::getName)
                .collect(Collectors.toList());
        Set<String> itemSetNotOnSale = customerStream
                .map(Customer::getWantToBuy)
                .flatMap(Collection::stream)
                .map(Item::getName)
                .filter(itemName -> !itemListOnSale.contains(itemName))
                .collect(Collectors.toSet());

        assertThat(itemSetNotOnSale, hasSize(3));
        assertThat(itemSetNotOnSale, hasItems("bag", "pants", "coat"));
    }

    @Difficult @Test
    public void havingEnoughMoney() {
        Stream<Customer> customerStream = this.mall.getCustomerList().stream();
        Stream<Shop> shopStream = this.mall.getShopList().stream();

        /**
         * Create a customer's name list including who are having enough money to buy all items they want which is on sale.
         * Items that are not on sale can be counted as 0 money cost.
         * If there is several same items with different prices, customer can choose the cheapest one.
         */
        Map<String, Integer> onSale = shopStream
                .map(Shop::getItemList)
                .flatMap(Collection::stream)
                .collect(toMap(Item::getName, Item::getPrice, (item1, item2) -> item1 < item2 ? item1 : item2));
        Predicate<Customer> havingEnoughMoney = customer ->
                customer.getWantToBuy().stream().mapToInt(item -> onSale.getOrDefault(item.getName(), 0)).sum() <= customer.getBudget();
        List<String> customerNameList = customerStream.filter(havingEnoughMoney).map(Customer::getName).collect(toList());

        assertThat(customerNameList, hasSize(7));
        assertThat(customerNameList, hasItems("Joe", "Patrick", "Chris", "Kathy", "Alice", "Andrew", "Amy"));
    }
}
