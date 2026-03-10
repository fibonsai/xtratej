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

import com.fibonsai.cryptomeria.xtratej.event.series.dao.DoubleTimeSeries;
import com.fibonsai.cryptomeria.xtratej.event.series.dao.TimeSeries;

import java.util.TreeMap;

public class DoubleTimeSeriesBuilder extends TimeSeriesBuilder<DoubleTimeSeriesBuilder> {

    private final TreeMap<Long, Double> doubles = new TreeMap<>();

    public DoubleTimeSeriesBuilder add(long timestamp, double value) {
        writeLock.lock();
        try {
            while (doubles.size() >= maxSize) {
                doubles.remove(doubles.firstKey());
            }
            doubles.put(timestamp, value);
        } finally {
            writeLock.unlock();
        }

        return this;
    }

    @Override
    public DoubleTimeSeries build() {
        readLock.lock();
        try {
            long[] _timestamps = doubles.sequencedKeySet().stream().mapToLong(Long::longValue).toArray();
            double[] _doubles = doubles.sequencedValues().stream().mapToDouble(Double::doubleValue).toArray();
            return new DoubleTimeSeries(id, _timestamps, _doubles);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public DoubleTimeSeriesBuilder from(TimeSeries timeSeries) {
        if (timeSeries instanceof DoubleTimeSeries(String id1, long[] timestamps, double[] values)) {
            for (int x = 0; x < timestamps.length; x++) {
                add(timestamps[x], values[x]);
                setId(id1);
            }
        }
        return this;
    }

    @Override
    public DoubleTimeSeriesBuilder merge(TimeSeries... timeSeriesArray) {
        for (var timeSeries: timeSeriesArray) {
            from(timeSeries);
        }
        return this;
    }
}
