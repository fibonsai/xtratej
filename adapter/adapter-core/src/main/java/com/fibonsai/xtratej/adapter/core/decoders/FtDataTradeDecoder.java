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
import com.fibonsai.xtratej.event.series.dao.builders.OrderTimeSeriesBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.fibonsai.xtratej.adapter.core.decoders.FtDataTradeDecoder.FIELD.*;
import static com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.TradeState.FILLED;

public class FtDataTradeDecoder implements Decoder {

    protected enum FIELD {
        TIMESTAMP("timestamp"),
        ID("id"),
        SIDE("side"),
        PRICE("price"),
        AMOUNT("amount"),
        ;

        private final String fieldStr;

        FIELD(String fieldStr) {
            this.fieldStr = fieldStr;
        }

        public String str() {
            return fieldStr;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FtDataTradeDecoder.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String tsId = Decoder.TS_ID_UNDEF;

    @Override
    public Decoder setId(String tsId) {
        this.tsId = tsId;
        return this;
    }

    @Override
    public OrderTimeSeries decode(ResultSet rs) {
        var builder = new OrderTimeSeriesBuilder().setId(tsId);
        try {
            String id = rs.getString(ID.str());
            BidAskSide side = switch (rs.getString(SIDE.str()).toUpperCase()) {
                case "BUY" -> BidAskSide.BID;
                case "SELL" -> BidAskSide.ASK;
                default -> throw new RuntimeException("Side unknown");
            };
            double price = rs.getDouble(PRICE.str());
            double amount = rs.getDouble(AMOUNT.str());
            long timestamp = rs.getLong(TIMESTAMP.str());
            builder.add(timestamp, id, side, FILLED, price, amount, amount);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return builder.build();
    }

    @Override
    public OrderTimeSeries decode(JsonNode jsonNode) {
        var builder = new OrderTimeSeriesBuilder().setId(tsId);
        String id = jsonNode.get(ID.str()).asString();
        String sideStr = jsonNode.get(SIDE.str()).asString().toUpperCase();
        BidAskSide side = switch (sideStr) {
            case "BUY" -> BidAskSide.BID;
            case "SELL" -> BidAskSide.ASK;
            default -> throw new RuntimeException("Side unknown: " + sideStr);
        };
        double price = jsonNode.get(PRICE.str()).asDouble();
        double amount = jsonNode.get(AMOUNT.str()).asDouble();
        long timestamp = jsonNode.get(TIMESTAMP.str()).asLong();
        builder.add(timestamp, id, side, FILLED, price, amount, amount);
        return builder.build();
    }

    @Override
    public OrderTimeSeries decode(String str) {
        return decode(MAPPER.readTree(str));
    }
}
