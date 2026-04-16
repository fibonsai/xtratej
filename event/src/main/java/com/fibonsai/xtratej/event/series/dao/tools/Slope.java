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

package com.fibonsai.xtratej.event.series.dao.tools;

import com.fibonsai.xtratej.event.series.dao.*;
import org.hipparchus.stat.regression.SimpleRegression;

public class Slope {

    private Slope() {}

    public static double from(TimeSeries timeSeries) {
        if (timeSeries.size() == 0) {
            return Double.NaN;
        }

        SimpleRegression regression = new SimpleRegression();
        long[] timestamps = timeSeries.timestamps();
        double[] values = switch (timeSeries) {
            case DoubleTimeSeries series -> series.values();
            case Double2TimeSeries series -> series.values();
            case BarTimeSeries series -> series.closes();
            case BandTimeSeries series -> series.middles();
            default -> {
                double[] doubles = new double[timestamps.length];
                for (int x=0; x < timestamps.length; x++) {
                    doubles[x] = Double.NaN;
                }
                yield doubles;
            }
        };
        for (int x = 0; x < values.length; x++) {
            double value = values[x];
            if (!Double.isNaN(value)) {
                regression.addData(timestamps[x], value);
            }
        }
        return regression.getSlope();
    }

}
