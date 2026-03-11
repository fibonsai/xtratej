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

import com.fibonsai.cryptomeria.xtratej.event.series.dao.OrderBookUpdateTimeSeries;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderBookUpdateTimeSeriesBuilderTest {

    @Test
    void testAddSingleElement() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder();
        OrderBookUpdateTimeSeries timeSeries = builder
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .setId("orderbook1")
            .build();

        long[] expectedTimestamps = {1000L};
        String[] expectedBidOrderIds = {"bid1"};
        String[] expectedAskOrderIds = {"ask1"};
        double[] expectedBidPrices = {100.0};
        double[] expectedAskPrices = {101.0};
        double[] expectedBidAmounts = {1.0};
        double[] expectedAskAmounts = {2.0};
        double[] expectedSpreads = {1.0};
        String expectedId = "orderbook1";

        assertEquals(expectedId, timeSeries.id());
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
        assertArrayEquals(expectedBidOrderIds, timeSeries.bidOrderIds());
        assertArrayEquals(expectedAskOrderIds, timeSeries.askOrderIds());
        assertArrayEquals(expectedBidPrices, timeSeries.bidPrices(), 0.0001);
        assertArrayEquals(expectedAskPrices, timeSeries.askPrices(), 0.0001);
        assertArrayEquals(expectedBidAmounts, timeSeries.bidAmounts(), 0.0001);
        assertArrayEquals(expectedAskAmounts, timeSeries.askAmounts(), 0.0001);
        assertArrayEquals(expectedSpreads, timeSeries.spreads(), 0.0001);
    }

    @Test
    void testAddMultipleElements() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder();
        OrderBookUpdateTimeSeries timeSeries = builder
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .add(3000L, "bid3", "ask3", 104.0, 105.0, 2.0, 3.0, 1.0)
            .setId("orderbook2")
            .build();

        assertEquals(3, timeSeries.size());
        assertEquals(3000L, timeSeries.timestamp());

        long[] expectedTimestamps = {1000L, 2000L, 3000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testTimestampSorted() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder().setMaxSize(10).setId("orderbook3");

        // Add elements out of order
        builder.add(3000L, "bid3", "ask3", 104.0, 105.0, 2.0, 3.0, 1.0);
        builder.add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0);
        builder.add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0);

        OrderBookUpdateTimeSeries timeSeries = builder.build();

        long[] expectedTimestamps = {1000L, 2000L, 3000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testMaxSizeOne() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder()
            .setId("orderbook4")
            .setMaxSize(1);

        OrderBookUpdateTimeSeries timeSeries = builder
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .build();

        assertNotEquals(2, timeSeries.size());
        assertEquals(1, timeSeries.size());

        long[] expectedTimestamps = {2000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testSetMaxSize() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder()
            .setId("orderbook5");

        OrderBookUpdateTimeSeries timeSeries = builder
            .setMaxSize(2)
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .build();

        assertEquals(2, timeSeries.size());

        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testOverwriteMaxSize() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder()
            .setMaxSize(3)
            .setId("orderbook6");

        OrderBookUpdateTimeSeries timeSeries = builder
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .add(3000L, "bid3", "ask3", 104.0, 105.0, 2.0, 3.0, 1.0)
            .add(4000L, "bid4", "ask4", 106.0, 107.0, 2.5, 3.5, 1.0)
            .build();

        assertEquals(3, timeSeries.size());

        long[] timestamps = timeSeries.timestamps();
        assertEquals(2000L, timestamps[0], "First timestamp should be 2000 (skipped 1000)");
        assertEquals(3000L, timestamps[1], "Second timestamp should be 3000");
        assertEquals(4000L, timestamps[2], "Third timestamp should be 4000 (appended)");
    }

    @Test
    void testEmptyBuilder() {
        OrderBookUpdateTimeSeries timeSeries = new OrderBookUpdateTimeSeriesBuilder().build();

        assertEquals(0, timeSeries.size());
        assertEquals(0, timeSeries.timestamps().length);
        assertEquals(0, timeSeries.bidOrderIds().length);
        assertEquals(0, timeSeries.askOrderIds().length);
        assertEquals(0, timeSeries.bidPrices().length);
        assertEquals(0, timeSeries.askPrices().length);
        assertEquals(0, timeSeries.bidAmounts().length);
        assertEquals(0, timeSeries.askAmounts().length);
        assertEquals(0, timeSeries.spreads().length);
    }

    @Test
    void testFromSameType() {
        OrderBookUpdateTimeSeriesBuilder builder1 = new OrderBookUpdateTimeSeriesBuilder();
        OrderBookUpdateTimeSeries original = builder1
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .setId("original")
            .build();

        OrderBookUpdateTimeSeriesBuilder builder2 = new OrderBookUpdateTimeSeriesBuilder();
        OrderBookUpdateTimeSeries copied = builder2.from(original).setId("copied").build();

        assertEquals("copied", copied.id());
        assertEquals(2, copied.size());
        assertArrayEquals(original.timestamps(), copied.timestamps());
        assertArrayEquals(original.bidOrderIds(), copied.bidOrderIds());
        assertArrayEquals(original.askOrderIds(), copied.askOrderIds());
        assertArrayEquals(original.bidPrices(), copied.bidPrices(), 0.0001);
        assertArrayEquals(original.askPrices(), copied.askPrices(), 0.0001);
        assertArrayEquals(original.bidAmounts(), copied.bidAmounts(), 0.0001);
        assertArrayEquals(original.askAmounts(), copied.askAmounts(), 0.0001);
        assertArrayEquals(original.spreads(), copied.spreads(), 0.0001);
    }

    @Test
    void testMergeMultiple() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder();

        OrderBookUpdateTimeSeries series1 = new OrderBookUpdateTimeSeriesBuilder()
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .setId("series1")
            .build();

        OrderBookUpdateTimeSeries series2 = new OrderBookUpdateTimeSeriesBuilder()
            .add(2000L, "bid2", "ask2", 102.0, 103.0, 1.5, 2.5, 1.0)
            .setId("series2")
            .build();

        OrderBookUpdateTimeSeries merged = builder.merge(series1, series2).setId("merged").build();

        assertEquals(2, merged.size());
        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, merged.timestamps());
    }

    @Test
    void testBidAskValuesMatch() {
        OrderBookUpdateTimeSeriesBuilder builder = new OrderBookUpdateTimeSeriesBuilder();
        OrderBookUpdateTimeSeries timeSeries = builder
            .add(1000L, "bid1", "ask1", 100.0, 101.0, 1.0, 2.0, 1.0)
            .setId("orderbook7")
            .build();

        // Verify that each row's values are consistent
        for (int i = 0; i < timeSeries.size(); i++) {
            double spread = timeSeries.spreads()[i];
            double expectedSpread = timeSeries.askPrices()[i] - timeSeries.bidPrices()[i];
            assertEquals(expectedSpread, spread, 0.0001, "Spread should equal askPrice - bidPrice");
        }
    }
}
