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

package com.fibonsai.xtratej.adapter.core.decoders;

import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries;
import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.BidAskSide;
import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.TradeState;
import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class FtDataTradeDecoderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FtDataTradeDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new FtDataTradeDecoder();
    }

    @Test
    void decode_withValidResultSet_shouldBuildOrderTimeSeries() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.getString("id")).thenReturn("trade-123");
        when(rs.getString("side")).thenReturn("BUY");
        when(rs.getDouble("price")).thenReturn(150.50);
        when(rs.getDouble("amount")).thenReturn(10.0);
        when(rs.getLong("timestamp")).thenReturn(1000000L);

        TimeSeries result = decoder.setId("test-id").decode(rs);
        OrderTimeSeries orderSeries = (OrderTimeSeries) result;

        assertNotNull(orderSeries);
        assertEquals("test-id", orderSeries.id());
        assertEquals(1, orderSeries.size());
        assertEquals(1000000L, orderSeries.timestamps()[0]);
        assertEquals("trade-123", orderSeries.orderIds()[0]);
        assertEquals(BidAskSide.BID, orderSeries.sides()[0]);
        assertEquals(TradeState.FILLED, orderSeries.tradeStates()[0]);
        assertEquals(150.50, orderSeries.prices()[0], 0.001);
        assertEquals(10.0, orderSeries.initialAmounts()[0], 0.001);
    }

    @Test
    void decode_withSellSide_inResultSet_shouldSetAsk() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.getString("id")).thenReturn("trade-456");
        when(rs.getString("side")).thenReturn("SELL");
        when(rs.getDouble("price")).thenReturn(151.00);
        when(rs.getDouble("amount")).thenReturn(5.5);
        when(rs.getLong("timestamp")).thenReturn(2000000L);

        OrderTimeSeries orderSeries = decoder.decode(rs);

        assertNotNull(orderSeries);
        assertEquals(BidAskSide.ASK, orderSeries.sides()[0]);
    }

    @Test
    void decode_withInvalidSide_inResultSet_shouldThrowException() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.getString("id")).thenReturn("trade-789");
        when(rs.getString("side")).thenReturn("UNKNOWN");

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            decoder.decode(rs);
        });
        assertTrue(exception.getMessage().contains("Side unknown"));
    }

    @Test
    void decode_withValidJsonNode_shouldBuildOrderTimeSeries() throws Exception {
        String json = """
            {
                "id": "trade-json-1",
                "side": "buy",
                "price": 200.75,
                "amount": 3.25,
                "timestamp": 3000000
            }
            """;
        JsonNode jsonNode = MAPPER.readTree(json);

        TimeSeries result = decoder.setId("json-test-id").decode(jsonNode);
        OrderTimeSeries orderSeries = (OrderTimeSeries) result;

        assertNotNull(orderSeries);
        assertEquals("json-test-id", orderSeries.id());
        assertEquals(1, orderSeries.size());
        assertEquals(3000000L, orderSeries.timestamps()[0]);
        assertEquals("trade-json-1", orderSeries.orderIds()[0]);
        assertEquals(BidAskSide.BID, orderSeries.sides()[0]);
        assertEquals(200.75, orderSeries.prices()[0], 0.001);
        assertEquals(3.25, orderSeries.initialAmounts()[0], 0.001);
    }

    @Test
    void decode_withValidString_shouldBuildOrderTimeSeries() throws Exception {
        String json = """
            {
                "id": "trade-string-1",
                "side": "SELL",
                "price": 300.00,
                "amount": 7.5,
                "timestamp": 4000000
            }
            """;

        TimeSeries result = decoder.setId("string-test-id").decode(json);
        OrderTimeSeries orderSeries = (OrderTimeSeries) result;

        assertNotNull(orderSeries);
        assertEquals("string-test-id", orderSeries.id());
        assertEquals(1, orderSeries.size());
        assertEquals(4000000L, orderSeries.timestamps()[0]);
        assertEquals("trade-string-1", orderSeries.orderIds()[0]);
        assertEquals(BidAskSide.ASK, orderSeries.sides()[0]);
    }

    @Test
    void setId_shouldReturnSelfForFluentAPI() {
        Decoder result = decoder.setId("new-id");
        assertSame(decoder, result);
    }
}
