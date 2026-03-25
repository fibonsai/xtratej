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
import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import com.fibonsai.xtratej.event.series.dao.builders.BooleanTimeSeriesBuilder;

import java.util.function.Function;

import static com.fibonsai.xtratej.event.series.dao.MyOrdersTimeSeries.BidAskSide.ASK;
import static com.fibonsai.xtratej.event.series.dao.MyOrdersTimeSeries.BidAskSide.BID;
import static com.fibonsai.xtratej.event.series.dao.MyOrdersTimeSeries.MINIMUM_AMOUNT_ALLOWED;

/***
 * this rule only supports MyOrderTimeSeries
 */
public class HasOpenPositionRule extends RuleStream<BooleanTimeSeries> {

    @Override
    protected Function<TimeSeries[], BooleanTimeSeries[]> predicate() {
        return timeSeriesArray -> {
            long lastTimestamp = 0;
            int count = 0;
            BooleanTimeSeries[] results = new BooleanTimeSeries[timeSeriesArray.length];
            for (var ts: timeSeriesArray) {
                if (ts.timestamp() > lastTimestamp) lastTimestamp = ts.timestamp();
                if (ts instanceof MyOrdersTimeSeries myorders) {
                    double remainAmount = getRemainAmount(myorders);
                    if (remainAmount < 0.0) {
                        results().emitError(new RuntimeException("Something is wrong. Selling more than you have."));
                        return BooleanTimeSeriesBuilder.toSingleArray(lastTimestamp, false);
                    }
                    results[count++] = new BooleanTimeSeriesBuilder()
                            .add(ts.timestamp(), remainAmount >= MINIMUM_AMOUNT_ALLOWED)
                            .build();
                }
            }
            if (count > 0) {
                return results;
            }

            return BooleanTimeSeriesBuilder.toSingleArray(lastTimestamp, false);
        };
    }

    private static double getRemainAmount(MyOrdersTimeSeries myorders) {
        double totalExecutedAmount = 0.0;
        double totalAmount = 0.0;
        double[] initialAmounts = myorders.initialAmounts();
        double[] executedAmounts = myorders.executedAmounts();
        for (int x = 0; x < myorders.size(); x++) {
            if (myorders.sides()[x] == ASK) {
                totalAmount -= initialAmounts[x];
                totalExecutedAmount -= executedAmounts[x];
            } else if (myorders.sides()[x] == BID) {
                totalAmount += initialAmounts[x];
                totalExecutedAmount += executedAmounts[x];
            }
        }
        if (totalAmount < 0.0 || totalExecutedAmount < 0.0) {
            return -1.0; // sold more than bought: error
        }
        return Math.max(totalAmount, totalExecutedAmount);
    }
}
