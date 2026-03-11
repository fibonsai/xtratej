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

import com.fibonsai.cryptomeria.xtratej.event.series.dao.BalanceUpdateTimeSeries;
import com.fibonsai.cryptomeria.xtratej.event.series.dao.BalanceUpdateTimeSeries.UpdateCause;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BalanceUpdateTimeSeriesBuilderTest {

    @Test
    void testAddSingleElement() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .setId("balance1")
            .build();

        long[] expectedTimestamps = {1000L};
        String[] expectedSymbols = {"BTC"};
        String[] expectedOwners = {"user1"};
        UpdateCause[] expectedCauses = {UpdateCause.DEPOSIT};
        double[] expectedTotals = {1000.0};
        double[] expectedAvailables = {1000.0};
        double[] expectedFrozens = {0.0};
        double[] expectedBorroweds = {0.0};
        double[] expectedLoaneds = {0.0};
        double[] expectedWithdrawings = {0.0};
        double[] expectedDepositing = {0.0};
        int[] expectedScales = {8};
        String expectedId = "balance1";

        assertEquals(expectedId, timeSeries.id());
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
        assertArrayEquals(expectedSymbols, timeSeries.symbols());
        assertArrayEquals(expectedOwners, timeSeries.owners());
        assertArrayEquals(expectedCauses, timeSeries.updateCauses());
        assertArrayEquals(expectedTotals, timeSeries.totals(), 0.0001);
        assertArrayEquals(expectedAvailables, timeSeries.availables(), 0.0001);
        assertArrayEquals(expectedFrozens, timeSeries.frozens(), 0.0001);
        assertArrayEquals(expectedBorroweds, timeSeries.borroweds(), 0.0001);
        assertArrayEquals(expectedLoaneds, timeSeries.loaneds(), 0.0001);
        assertArrayEquals(expectedWithdrawings, timeSeries.withdrawings(), 0.0001);
        assertArrayEquals(expectedDepositing, timeSeries.depositings(), 0.0001);
        assertArrayEquals(expectedScales, timeSeries.scales());
    }

    @Test
    void testAddMultipleElements() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(3000L, "BTC", "user1", UpdateCause.WITHDRAW, 950.0, 900.0, 50.0, 0.0, 0.0, 0.0, 0.0, 8)
            .setId("balance2")
            .build();

        assertEquals(3, timeSeries.size());
        assertEquals(3000L, timeSeries.timestamp());
    }

    @Test
    void testTimestampSorted() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder().setMaxSize(10).setId("balance3");

        builder.add(3000L, "BTC", "user1", UpdateCause.WITHDRAW, 950.0, 900.0, 50.0, 0.0, 0.0, 0.0, 0.0, 8);
        builder.add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8);
        builder.add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8);

        BalanceUpdateTimeSeries timeSeries = builder.build();

        long[] expectedTimestamps = {1000L, 2000L, 3000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());

        // Verify data alignment after sorting
        assertEquals("BTC", timeSeries.symbols()[0]);
        assertEquals("ETH", timeSeries.symbols()[1]);
        assertEquals("BTC", timeSeries.symbols()[2]);
    }

    @Test
    void testMaxSizeOne() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder()
            .setId("balance4")
            .setMaxSize(1);

        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .build();

        assertNotEquals(2, timeSeries.size());
        assertEquals(1, timeSeries.size());
    }

    @Test
    void testSetMaxSize() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder()
            .setId("balance5");

        BalanceUpdateTimeSeries timeSeries = builder
            .setMaxSize(2)
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .build();

        assertEquals(2, timeSeries.size());

        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, timeSeries.timestamps());
    }

    @Test
    void testOverwriteMaxSize() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder()
            .setMaxSize(3)
            .setId("balance6");

        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(3000L, "BTC", "user1", UpdateCause.WITHDRAW, 950.0, 900.0, 50.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(4000L, "SOL", "user2", UpdateCause.TRANSFER, 200.0, 200.0, 0.0, 0.0, 0.0, 0.0, 0.0, 9)
            .build();

        assertEquals(3, timeSeries.size());

        long[] timestamps = timeSeries.timestamps();
        assertEquals(2000L, timestamps[0]);
        assertEquals(3000L, timestamps[1]);
        assertEquals(4000L, timestamps[2]);
    }

    @Test
    void testEmptyBuilder() {
        BalanceUpdateTimeSeries timeSeries = new BalanceUpdateTimeSeriesBuilder().build();

        assertEquals(0, timeSeries.size());
        assertEquals(0, timeSeries.timestamps().length);
        assertEquals(0, timeSeries.symbols().length);
        assertEquals(0, timeSeries.owners().length);
        assertEquals(0, timeSeries.updateCauses().length);
        assertEquals(0, timeSeries.totals().length);
        assertEquals(0, timeSeries.availables().length);
        assertEquals(0, timeSeries.frozens().length);
        assertEquals(0, timeSeries.borroweds().length);
        assertEquals(0, timeSeries.loaneds().length);
        assertEquals(0, timeSeries.withdrawings().length);
        assertEquals(0, timeSeries.depositings().length);
        assertEquals(0, timeSeries.scales().length);
    }

    @Test
    void testFromSameType() {
        BalanceUpdateTimeSeriesBuilder builder1 = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries original = builder1
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .setId("original")
            .build();

        BalanceUpdateTimeSeriesBuilder builder2 = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries copied = builder2.from(original).setId("copied").build();

        assertEquals("copied", copied.id());
        assertEquals(2, copied.size());
        assertArrayEquals(original.timestamps(), copied.timestamps());
        assertArrayEquals(original.symbols(), copied.symbols());
        assertArrayEquals(original.owners(), copied.owners());
        assertArrayEquals(original.updateCauses(), copied.updateCauses());
        assertArrayEquals(original.totals(), copied.totals(), 0.0001);
        assertArrayEquals(original.availables(), copied.availables(), 0.0001);
        assertArrayEquals(original.frozens(), copied.frozens(), 0.0001);
        assertArrayEquals(original.borroweds(), copied.borroweds(), 0.0001);
        assertArrayEquals(original.loaneds(), copied.loaneds(), 0.0001);
        assertArrayEquals(original.withdrawings(), copied.withdrawings(), 0.0001);
        assertArrayEquals(original.depositings(), copied.depositings(), 0.0001);
        assertArrayEquals(original.scales(), copied.scales());
    }

    @Test
    void testMergeMultiple() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();

        BalanceUpdateTimeSeries series1 = new BalanceUpdateTimeSeriesBuilder()
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .setId("series1")
            .build();

        BalanceUpdateTimeSeries series2 = new BalanceUpdateTimeSeriesBuilder()
            .add(2000L, "ETH", "user1", UpdateCause.TRADE, 500.0, 400.0, 100.0, 0.0, 0.0, 0.0, 0.0, 8)
            .setId("series2")
            .build();

        BalanceUpdateTimeSeries merged = builder.merge(series1, series2).setId("merged").build();

        assertEquals(2, merged.size());
        long[] expectedTimestamps = {1000L, 2000L};
        assertArrayEquals(expectedTimestamps, merged.timestamps());
    }

    @Test
    void testUpdateCauseValues() {
        // Test all UpdateCause enum values
        for (UpdateCause cause : UpdateCause.values()) {
            BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
            BalanceUpdateTimeSeries timeSeries = builder
                .add(1000L, "BTC", "user1", cause, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
                .build();

            assertEquals(cause, timeSeries.updateCauses()[0]);
        }
    }

    @Test
    void testBalanceEquations() {
        // Test that total = available + frozen
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 800.0, 200.0, 0.0, 0.0, 0.0, 0.0, 8)
            .build();

        for (int i = 0; i < timeSeries.size(); i++) {
            double total = timeSeries.totals()[i];
            double available = timeSeries.availables()[i];
            double frozen = timeSeries.frozens()[i];
            assertEquals(total, available + frozen, 0.0001, "total should equal available + frozen");
        }
    }

    @Test
    void testZeroScale() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)
            .build();

        assertEquals(0, timeSeries.scales()[0]);
    }

    @Test
    void testLargeScales() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.DEPOSIT, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 18)
            .build();

        assertEquals(18, timeSeries.scales()[0]);
    }

    @Test
    void testTransferCause() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.TRANSFER, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .build();

        assertEquals(UpdateCause.TRANSFER, timeSeries.updateCauses()[0]);
    }

    @Test
    void testUnknownCause() {
        BalanceUpdateTimeSeriesBuilder builder = new BalanceUpdateTimeSeriesBuilder();
        BalanceUpdateTimeSeries timeSeries = builder
            .add(1000L, "BTC", "user1", UpdateCause.UNKNOWN, 1000.0, 1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8)
            .build();

        assertEquals(UpdateCause.UNKNOWN, timeSeries.updateCauses()[0]);
    }
}
