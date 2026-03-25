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

package com.fibonsai.xtratej.engine.rules.impl;

import com.fibonsai.xtratej.engine.rules.RuleStream;
import com.fibonsai.xtratej.event.series.dao.BooleanTimeSeries;
import com.fibonsai.xtratej.event.series.dao.MyOrdersTimeSeries;
import com.fibonsai.xtratej.event.series.dao.MyOrdersTimeSeries.TradeState;
import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import com.fibonsai.xtratej.event.series.dao.builders.BooleanTimeSeriesBuilder;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.function.Function;

/***
 * this rule only supports MyOrderTimeSeries
 */
public class ActiveOrderRule extends RuleStream<BooleanTimeSeries> {

    public static final List<TradeState> ACTIVE_STATES = List.of(
            TradeState.NEW,
            TradeState.OPEN,
            TradeState.PARTIALLY_FILLED,
            TradeState.PARTIALLY_CANCELED,
            TradeState.PENDING_REPLACE,
            TradeState.PENDING_NEW,
            TradeState.REPLACED,
            TradeState.STOPPED
    );

    private int min = 0;
    private int max = Integer.MAX_VALUE;

    @Override
    public RuleStream<BooleanTimeSeries> setParams(JsonNode params) {
        for (var e: params.properties()) {
            if ("min".equals(e.getKey()) && e.getValue().isInt()) {
                this.min = e.getValue().asInt();
            }
            if ("max".equals(e.getKey()) && e.getValue().isInt()) {
                this.max = e.getValue().asInt();
            }
        }
        return this;
    }

    @Override
    protected Function<TimeSeries[], BooleanTimeSeries[]> predicate() {
        return timeSeriesArray -> {
            BooleanTimeSeries[] results = new BooleanTimeSeries[timeSeriesArray.length];
            long lastTimestamp = 0;
            int count = 0;
            for (var ts: timeSeriesArray) {
                if (ts.timestamp() > lastTimestamp) lastTimestamp = ts.timestamp();
                if (ts instanceof MyOrdersTimeSeries myOrders) {
                    long activeOrders = 0;
                    for (var state: myOrders.tradeStates()) {
                        if (ACTIVE_STATES.contains(state)) activeOrders++;
                    }
                    results[count++] = new BooleanTimeSeriesBuilder()
                            .add(ts.timestamp(), activeOrders >= min && activeOrders <= max)
                            .build();
                } else {
                    results[count++] = new BooleanTimeSeriesBuilder()
                            .add(ts.timestamp(), false)
                            .build();
                }
            }
            if (count > 0) {
                return results;
            }

            return BooleanTimeSeriesBuilder.toSingleArray(lastTimestamp, false);
        };
    }
}
