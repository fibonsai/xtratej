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
import com.fibonsai.xtratej.event.series.dao.*;
import com.fibonsai.xtratej.event.series.dao.builders.BooleanTimeSeriesBuilder;
import tools.jackson.databind.JsonNode;

import java.util.function.Function;

public class MaxDrawdownRule extends RuleStream<BooleanTimeSeries> {

    private double max = Double.POSITIVE_INFINITY;

    @Override
    public RuleStream<BooleanTimeSeries> setParams(JsonNode params) {
        for (var e: params.properties()) {
            if ("max".equals(e.getKey()) && e.getValue().isDouble()) {
                this.max = e.getValue().asDouble();
            }
        }
        return this;
    }

    @Override
    protected Function<TimeSeries[], BooleanTimeSeries[]> predicate() {
        return timeSeriesArray -> {
            long lastTimestamp = 0;
            int count = 0;
            BooleanTimeSeries[] results = new BooleanTimeSeries[timeSeriesArray.length];
            for (var ts: timeSeriesArray) {
                if (ts.timestamp() > lastTimestamp) lastTimestamp = ts.timestamp();
                double peak = 0.0;
                double low = Double.MAX_VALUE;
                for (int x = 0; x < ts.size(); x++) {
                    double price = switch (ts) {
                        case MyOrdersTimeSeries myOrders -> myOrders.prices()[x];
                        case DoubleTimeSeries dts -> dts.values()[x];
                        case Double2TimeSeries dts -> dts.values()[x];
                        case BarTimeSeries barTs -> barTs.closes()[x];
                        case BandTimeSeries bandTs -> bandTs.middles()[x];
                        default -> 0.0;
                    };
                    if (price > peak) peak = price;
                    if (price < low) low = price;
                }
                if (low == Double.MAX_VALUE) low = 0.0;
                double maxDrawdown = (peak == 0.0) ? Double.POSITIVE_INFINITY : 1.0 - ((low - peak) / peak);

                results[count++] = new BooleanTimeSeriesBuilder()
                        .add(ts.timestamp(), maxDrawdown < max)
                        .build();
            }
            if (count > 0) {
                return results;
            }

            return BooleanTimeSeriesBuilder.toSingleArray(lastTimestamp, false);
        };
    }
}
