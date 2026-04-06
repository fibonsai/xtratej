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

import com.fibonsai.xtratej.event.series.dao.BarTimeSeries;
import com.fibonsai.xtratej.event.series.dao.builders.BarTimeSeriesBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;

import static com.fibonsai.xtratej.adapter.core.decoders.FtDataCandlestickDecoder.FIELD.*;

public class FtDataCandlestickDecoder implements Decoder {

    protected enum FIELD {
        INSTANT("date"),
        OPEN("open"),
        HIGH("high"),
        LOW("low"),
        CLOSE("close"),
        VOLUME("volume"),
        ;

        private final String fieldStr;

        FIELD(String fieldStr) {
            this.fieldStr = fieldStr;
        }

        public String str() {
            return fieldStr;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FtDataCandlestickDecoder.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String tsId = Decoder.TS_ID_UNDEF;

    @Override
    public Decoder setId(String tsId) {
        this.tsId = tsId;
        return this;
    }

    @Override
    public BarTimeSeries decode(ResultSet rs) {
        var builder = new BarTimeSeriesBuilder().setId(tsId);
        try {
            long timestamp = rs.getLong(INSTANT.str()) / 1_000;
            double open = rs.getDouble(OPEN.str());
            double high = rs.getDouble(HIGH.str());
            double low = rs.getDouble(LOW.str());
            double close = rs.getDouble(CLOSE.str());
            double volume = rs.getDouble(VOLUME.str());
            builder.add(timestamp, open, high, low, close, volume);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return builder.build();
    }

    @Override
    public BarTimeSeries decode(JsonNode jsonNode) {
        var builder = new BarTimeSeriesBuilder().setId(tsId);
        try {
            long timestamp = jsonNode.get(INSTANT.str()).asLong();
            double open = jsonNode.get(OPEN.str()).asDouble();
            double high = jsonNode.get(HIGH.str()).asDouble();
            double low = jsonNode.get(LOW.str()).asDouble();
            double close = jsonNode.get(CLOSE.str()).asDouble();
            double volume = jsonNode.get(VOLUME.str()).asDouble();
            builder.add(timestamp, open, high, low, close, volume);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return builder.build();
    }

    @Override
    public BarTimeSeries decode(String str) {
        return decode(MAPPER.readTree(str));
    }
}
