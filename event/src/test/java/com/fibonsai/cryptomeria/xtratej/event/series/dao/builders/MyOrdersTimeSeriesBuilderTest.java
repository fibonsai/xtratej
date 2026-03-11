/*
 *  Copyright (c) 2026 fibonsai.com
 *  All rights reserved.
 *
 *  This source is subject to the Apache License, Version 2.0.
 *  Please see the LICENSE file for more information.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.fibonsai.cryptomeria.xtratej.event.series.dao.builders;

import com.fibonsai.cryptomeria.xtratej.event.series.dao.MyOrdersTimeSeries;
import com.fibonsai.cryptomeria.xtratej.event.series.dao.MyOrdersTimeSeries.OrderCondition;
import com.fibonsai.cryptomeria.xtratej.event.series.dao.MyOrdersTimeSeries.OrderType;
import com.fibonsai.cryptomeria.xtratej.event.series.dao.MyOrdersTimeSeries.TradeState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MyOrdersTimeSeriesBuilderTest {

    @Test
    void testAddSingleElement() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "test rule")
            .setId("myorders1")
            .build();

        long[] expectedTimestamps = {1000L};
        String[] expectedOrderIds = {"order1"};
        String[] expectedSymbols = {"BTC-USD"};
        String[] expectedOwners = {"user1"};
        TradeState[] expectedTradeStates = {TradeState.NEW};
        OrderType[] expectedOrderTypes = {OrderType.LIMIT};
        double[] expectedFees = {0.1};
        double[] expectedPrices = {50000.0};
        double[] expectedLimitPrices = {50000.0};
        double[] expectedStopPrices = {0.0};
        double[] expectedTakeProfitPrices = {0.0};
        double[] expectedTrailingPrices = {0.0};
        double[] expectedInitialAmounts = {1.0};
        double[] expectedExecutedAmounts = {0.0};
        OrderCondition[] expectedOrderConditions = {OrderCondition.DAY};
        String[] expectedOrderConditionRules = {"test rule"};
        String expectedId = "myorders1";

        assertEquals(expectedId, timeSeries.id());
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
        assertArrayEquals(expectedOrderIds, timeSeries.orderIds());
        assertArrayEquals(expectedSymbols, timeSeries.symbols());
        assertArrayEquals(expectedOwners, timeSeries.owners());
        assertArrayEquals(expectedTradeStates, timeSeries.tradeStates());
        assertArrayEquals(expectedOrderTypes, timeSeries.orderTypes());
        assertArrayEquals(expectedFees, timeSeries.fees(), 0.0001);
        assertArrayEquals(expectedPrices, timeSeries.prices(), 0.0001);
        assertArrayEquals(expectedLimitPrices, timeSeries.limitPrices(), 0.0001);
        assertArrayEquals(expectedStopPrices, timeSeries.stopPrices(), 0.0001);
        assertArrayEquals(expectedTakeProfitPrices, timeSeries.takeProfitPrices(), 0.0001);
        assertArrayEquals(expectedTrailingPrices, timeSeries.trailingPrices(), 0.0001);
        assertArrayEquals(expectedInitialAmounts, timeSeries.initialAmounts(), 0.0001);
        assertArrayEquals(expectedExecutedAmounts, timeSeries.executedAmounts(), 0.0001);
        assertArrayEquals(expectedOrderConditions, timeSeries.orderConditions());
        assertArrayEquals(expectedOrderConditionRules, timeSeries.orderConditionsRules());
    }

    @Test
    void testAddMultipleElements() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "rule1")
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "rule2")
            .add(3000L, "order3", "BTC-USD", "user1", TradeState.FILLED, OrderType.STOP,
                0.15, 51000.0, 51000.0, 50500.0, 0.0, 0.0, 1.0, 1.0,
                OrderCondition.DAY, "rule3")
            .setId("myorders2")
            .build();

        assertEquals(3, timeSeries.size());
        assertEquals(3000L, timeSeries.timestamp());

        long[] expectedTimestamps = {1000L, 2000L, 3000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());

        // Check values at specific indices
        assertEquals("order2", timeSeries.orderIds()[1]);
        assertEquals("ETH-USD", timeSeries.symbols()[1]);
        assertEquals(TradeState.PARTIALLY_FILLED, timeSeries.tradeStates()[1]);
        assertEquals(OrderType.MARKET, timeSeries.orderTypes()[1]);
        assertEquals(3000.0, timeSeries.prices()[1], 0.0001);
    }

    @Test
    void testTimestampSorted() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder().setMaxSize(10).setId("myorders3");

        // Add elements out of order
        builder.add(3000L, "order3", "BTC-USD", "user1", TradeState.FILLED, OrderType.LIMIT,
            0.15, 51000.0, 51000.0, 50500.0, 0.0, 0.0, 1.0, 1.0,
            OrderCondition.DAY, "rule3");
        builder.add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
            0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
            OrderCondition.DAY, "rule1");
        builder.add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
            0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
            OrderCondition.GOOD_TIL_CANCELED, "rule2");

        MyOrdersTimeSeries timeSeries = builder.build();

        long[] expectedTimestamps = {1000L, 2000L, 3000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());

        // Verify data remains aligned after sorting
        assertEquals("order1", timeSeries.orderIds()[0]);
        assertEquals("order2", timeSeries.orderIds()[1]);
        assertEquals("order3", timeSeries.orderIds()[2]);
    }

    @Test
    void testMaxSizeOne() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder()
            .setId("myorders4")
            .setMaxSize(1);

        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "rule1")
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "rule2")
            .build();

        assertNotEquals(2, timeSeries.size());
        assertEquals(1, timeSeries.size());

        long[] expectedTimestamps = {2000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
        assertEquals("order2", timeSeries.orderIds()[0]);
    }

    @Test
    void testSetMaxSize() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder()
            .setId("myorders5");

        MyOrdersTimeSeries timeSeries = builder
            .setMaxSize(2)
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "rule1")
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "rule2")
            .build();

        assertEquals(2, timeSeries.size());

        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testOverwriteMaxSize() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder()
            .setMaxSize(3)
            .setId("myorders6");

        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "rule1")
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "rule2")
            .add(3000L, "order3", "BTC-USD", "user1", TradeState.FILLED, OrderType.STOP,
                0.15, 51000.0, 51000.0, 50500.0, 0.0, 0.0, 1.0, 1.0,
                OrderCondition.DAY, "rule3")
            .add(4000L, "order4", "SOL-USD", "user3", TradeState.NEW, OrderType.LIMIT,
                0.05, 100.0, 100.0, 0.0, 0.0, 0.0, 10.0, 0.0,
                OrderCondition.DAY, "rule4")
            .build();

        assertEquals(3, timeSeries.size());

        long[] timestamps = timeSeries.timestamps();
        assertEquals(2000L, timestamps[0], "First timestamp should be 2000 (skipped 1000)");
        assertEquals(3000L, timestamps[1], "Second timestamp should be 3000");
        assertEquals(4000L, timestamps[2], "Third timestamp should be 4000 (appended)");
    }

    @Test
    void testEmptyBuilder() {
        MyOrdersTimeSeries timeSeries = new MyOrdersTimeSeriesBuilder().build();

        assertEquals(0, timeSeries.size());
        assertEquals(0, timeSeries.timestamps().length);
        assertEquals(0, timeSeries.orderIds().length);
        assertEquals(0, timeSeries.symbols().length);
        assertEquals(0, timeSeries.owners().length);
        assertEquals(0, timeSeries.tradeStates().length);
        assertEquals(0, timeSeries.orderTypes().length);
        assertEquals(0, timeSeries.fees().length);
        assertEquals(0, timeSeries.prices().length);
        assertEquals(0, timeSeries.limitPrices().length);
        assertEquals(0, timeSeries.stopPrices().length);
        assertEquals(0, timeSeries.takeProfitPrices().length);
        assertEquals(0, timeSeries.trailingPrices().length);
        assertEquals(0, timeSeries.initialAmounts().length);
        assertEquals(0, timeSeries.executedAmounts().length);
        assertEquals(0, timeSeries.orderConditions().length);
        assertEquals(0, timeSeries.orderConditionsRules().length);
    }

    @Test
    void testFromSameType() {
        MyOrdersTimeSeriesBuilder builder1 = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries original = builder1
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "test rule")
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "test rule2")
            .setId("original")
            .build();

        MyOrdersTimeSeriesBuilder builder2 = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries copied = builder2.from(original).setId("copied").build();

        assertEquals("copied", copied.id());
        assertEquals(2, copied.size());
        assertArrayEquals(original.timestamps(), copied.timestamps());
        assertArrayEquals(original.orderIds(), copied.orderIds());
        assertArrayEquals(original.symbols(), copied.symbols());
        assertArrayEquals(original.owners(), copied.owners());
        assertArrayEquals(original.tradeStates(), copied.tradeStates());
        assertArrayEquals(original.orderTypes(), copied.orderTypes());
        assertArrayEquals(original.fees(), copied.fees(), 0.0001);
        assertArrayEquals(original.prices(), copied.prices(), 0.0001);
        assertArrayEquals(original.limitPrices(), copied.limitPrices(), 0.0001);
        assertArrayEquals(original.stopPrices(), copied.stopPrices(), 0.0001);
        assertArrayEquals(original.takeProfitPrices(), copied.takeProfitPrices(), 0.0001);
        assertArrayEquals(original.trailingPrices(), copied.trailingPrices(), 0.0001);
        assertArrayEquals(original.initialAmounts(), copied.initialAmounts(), 0.0001);
        assertArrayEquals(original.executedAmounts(), copied.executedAmounts(), 0.0001);
        assertArrayEquals(original.orderConditions(), copied.orderConditions());
        assertArrayEquals(original.orderConditionsRules(), copied.orderConditionsRules());
    }

    @Test
    void testMergeMultiple() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();

        MyOrdersTimeSeries series1 = new MyOrdersTimeSeriesBuilder()
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "rule1")
            .setId("series1")
            .build();

        MyOrdersTimeSeries series2 = new MyOrdersTimeSeriesBuilder()
            .add(2000L, "order2", "ETH-USD", "user2", TradeState.PARTIALLY_FILLED, OrderType.MARKET,
                0.2, 3000.0, 3000.0, 0.0, 0.0, 0.0, 0.5, 0.25,
                OrderCondition.GOOD_TIL_CANCELED, "rule2")
            .setId("series2")
            .build();

        MyOrdersTimeSeries merged = builder.merge(series1, series2).setId("merged").build();

        assertEquals(2, merged.size());
        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, merged.timestamps());
    }

    @Test
    void testTradeStateValues() {
        // Test all TradeState enum values
        for (TradeState state : TradeState.values()) {
            MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
            MyOrdersTimeSeries timeSeries = builder
                .add(1000L, "order1", "BTC-USD", "user1", state, OrderType.LIMIT,
                    0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                    OrderCondition.DAY, "test")
                .build();

            assertEquals(state, timeSeries.tradeStates()[0]);
        }
    }

    @Test
    void testOrderTypeValues() {
        // Test all OrderType enum values
        for (OrderType type : OrderType.values()) {
            MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
            MyOrdersTimeSeries timeSeries = builder
                .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, type,
                    0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                    OrderCondition.DAY, "test")
                .build();

            assertEquals(type, timeSeries.orderTypes()[0]);
        }
    }

    @Test
    void testOrderConditionValues() {
        // Test all OrderCondition enum values
        for (OrderCondition condition : OrderCondition.values()) {
            MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
            MyOrdersTimeSeries timeSeries = builder
                .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.LIMIT,
                    0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                    condition, "test rule")
                .build();

            assertEquals(condition, timeSeries.orderConditions()[0]);
        }
    }

    @Test
    void testTrailingStopWithTrailValue() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.TRAILING_STOP,
                0.0, 50000.0, 50000.0, 0.0, 0.0, 100.0, 1.0, 0.0,
                OrderCondition.DAY, "trailing rule")
            .build();

        assertEquals(100.0, timeSeries.trailingPrices()[0], 0.0001);
        assertEquals(0.0, timeSeries.stopPrices()[0], 0.0001);
        assertEquals(0.0, timeSeries.takeProfitPrices()[0], 0.0001);
    }

    @Test
    void testStopLimitOrderWithStopPrice() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.NEW, OrderType.STOP,
                0.0, 50000.0, 50000.0, 49500.0, 0.0, 0.0, 1.0, 0.0,
                OrderCondition.DAY, "stop rule")
            .build();

        assertEquals(49500.0, timeSeries.stopPrices()[0], 0.0001);
        assertEquals(50000.0, timeSeries.limitPrices()[0], 0.0001);
        assertEquals(50000.0, timeSeries.prices()[0], 0.0001);
    }

    @Test
    void testPartiallyFilledOrder() {
        MyOrdersTimeSeriesBuilder builder = new MyOrdersTimeSeriesBuilder();
        MyOrdersTimeSeries timeSeries = builder
            .add(1000L, "order1", "BTC-USD", "user1", TradeState.PARTIALLY_FILLED, OrderType.LIMIT,
                0.1, 50000.0, 50000.0, 0.0, 0.0, 0.0, 1.0, 0.5,
                OrderCondition.DAY, "test")
            .build();

        assertEquals(1.0, timeSeries.initialAmounts()[0], 0.0001);
        assertEquals(0.5, timeSeries.executedAmounts()[0], 0.0001);
        assertEquals(TradeState.PARTIALLY_FILLED, timeSeries.tradeStates()[0]);
    }
}
