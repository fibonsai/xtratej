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
import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries;
import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.BidAskSide;
import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import com.fibonsai.xtratej.event.series.dao.builders.BooleanTimeSeriesBuilder;
import tools.jackson.databind.JsonNode;

import java.util.function.Function;

import static com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.BidAskSide.ASK;
import static com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.BidAskSide.BID;
import static com.fibonsai.xtratej.event.series.dao.OrderTimeSeries.MINIMUM_AMOUNT_ALLOWED;

/**
 * GainLossRatioRule calculates the ratio of winning trades to losing trades.
 * - A "winning" trade is one where the profit is positive (result > 0)
 * - A "losing" trade is one where the profit is negative (result < 0)
 * - The ratio is: gainCounter / lossCounter
 * - If no losses, ratio is POSITIVE_INFINITY
 * - If no gains and no losses, ratio is 1.0
 *
 * IMPORTANT: this rule only supports MyOrderTimeSeries
*/
public class GainLossRatioRule extends RuleStream<BooleanTimeSeries> {

    private double ratioMin = Double.NEGATIVE_INFINITY;
    private double ratioMax = Double.POSITIVE_INFINITY;

    @Override
    public RuleStream<BooleanTimeSeries> setParams(JsonNode params) {
        for (var e: params.properties()) {
            if ("ratioMin".equals(e.getKey()) && e.getValue().isDouble()) {
                this.ratioMin = e.getValue().asDouble();
            }
            if ("ratioMax".equals(e.getKey()) && e.getValue().isDouble()) {
                this.ratioMax = e.getValue().asDouble();
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
                if (ts instanceof OrderTimeSeries myorders) {
                    double result = Double.NaN;
                    double amountAccumulated = Double.NaN;
                    float gainCounter = 0.0F;
                    float lossCounter = 0.0F;
                    for (int x = 0; x < myorders.size(); x++) {
                        BidAskSide side = myorders.sides()[x];
                        double amount = myorders.executedAmounts()[x];
                        double price = myorders.prices()[x];
                        if (Double.isNaN(result) && side == ASK) continue;
                        if (side == ASK) {
                            result = Double.isNaN(result) ? (amount * price) : result + (amount * price);
                            amountAccumulated = Double.isNaN(amountAccumulated) ? amount : amountAccumulated - amount;
                            if (amountAccumulated < 0.0) {
                                results().emitError(new RuntimeException("Something is wrong. Selling more than you have."));
                                amountAccumulated = 0.0;
                            }
                            if (amountAccumulated < MINIMUM_AMOUNT_ALLOWED) {
                                if (result > 0) gainCounter++;
                                if (result < 0) lossCounter++;
                                amountAccumulated = Double.NaN;
                                result = Double.NaN;
                            }
                        } else if (side == BID) {
                            result = Double.isNaN(result) ? amount * price * -1.0 : result - (amount * price);
                            amountAccumulated = Double.isNaN(amountAccumulated) ? amount : amountAccumulated + amount;
                        }
                    }

                    double ratio = (lossCounter == 0.0 && gainCounter > 0.0) ?
                            Double.POSITIVE_INFINITY :
                            ((gainCounter == lossCounter) ? 1.0 : gainCounter / lossCounter);

                    results[count++] = new BooleanTimeSeriesBuilder()
                            .add(ts.timestamp(), ratio >= ratioMin && ratio <= ratioMax)
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
