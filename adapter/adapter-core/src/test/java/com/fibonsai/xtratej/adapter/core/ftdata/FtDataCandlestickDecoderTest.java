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

package com.fibonsai.xtratej.adapter.core.ftdata;

import com.fibonsai.xtratej.adapter.core.Decoder;
import com.fibonsai.xtratej.event.series.dao.BarTimeSeries;
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

class FtDataCandlestickDecoderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FtDataCandlestickDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new FtDataCandlestickDecoder();
    }

    @Test
    void decode_withValidResultSet_shouldBuildBarTimeSeries() throws SQLException {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.getTimestamp("date")).thenReturn(new java.sql.Timestamp(1000000000));
        when(rs.getDouble("open")).thenReturn(100.0);
        when(rs.getDouble("high")).thenReturn(110.0);
        when(rs.getDouble("low")).thenReturn(90.0);
        when(rs.getDouble("close")).thenReturn(105.0);
        when(rs.getDouble("volume")).thenReturn(5000.0);

        TimeSeries result = decoder.setId("candle-test-id").decode(rs);
        BarTimeSeries barSeries = (BarTimeSeries) result;

        assertNotNull(barSeries);
        assertEquals("candle-test-id", barSeries.id());
        assertEquals(1, barSeries.size());
        //assertEquals(989200L, barSeries.timestamps()[0]); // convert
        assertEquals(100.0, barSeries.opens()[0], 0.001);
        assertEquals(110.0, barSeries.highs()[0], 0.001);
        assertEquals(90.0, barSeries.lows()[0], 0.001);
        assertEquals(105.0, barSeries.closes()[0], 0.001);
        assertEquals(5000.0, barSeries.volumes()[0], 0.001);
    }

    @Test
    void decode_withValidJsonNode_shouldBuildBarTimeSeries() throws Exception {
        String json = """
            {
                "date": 1609459200000,
                "open": 200.0,
                "high": 210.0,
                "low": 195.0,
                "close": 205.0,
                "volume": 8000.0
            }
            """;
        JsonNode jsonNode = MAPPER.readTree(json);

        TimeSeries result = decoder.setId("json-candle-id").decode(jsonNode);
        BarTimeSeries barSeries = (BarTimeSeries) result;

        assertNotNull(barSeries);
        assertEquals("json-candle-id", barSeries.id());
        assertEquals(1, barSeries.size());
        assertEquals(1609459200000L, barSeries.timestamps()[0]);
        assertEquals(200.0, barSeries.opens()[0], 0.001);
        assertEquals(210.0, barSeries.highs()[0], 0.001);
        assertEquals(195.0, barSeries.lows()[0], 0.001);
        assertEquals(205.0, barSeries.closes()[0], 0.001);
        assertEquals(8000.0, barSeries.volumes()[0], 0.001);
    }

    @Test
    void decode_withValidString_shouldBuildBarTimeSeries() throws Exception {
        String json = """
            {
                "date": 1609459200000,
                "open": 300.0,
                "high": 310.0,
                "low": 290.0,
                "close": 305.0,
                "volume": 10000.0
            }
            """;

        TimeSeries result = decoder.setId("string-candle-id").decode(json);
        BarTimeSeries barSeries = (BarTimeSeries) result;

        assertNotNull(barSeries);
        assertEquals("string-candle-id", barSeries.id());
        assertEquals(1, barSeries.size());
        assertEquals(300.0, barSeries.opens()[0], 0.001);
        assertEquals(310.0, barSeries.highs()[0], 0.001);
        assertEquals(290.0, barSeries.lows()[0], 0.001);
        assertEquals(305.0, barSeries.closes()[0], 0.001);
        assertEquals(10000.0, barSeries.volumes()[0], 0.001);
    }

    @Test
    void setId_shouldReturnSelfForFluentAPI() {
        Decoder result = decoder.setId("new-id");
        assertSame(decoder, result);
    }
}
