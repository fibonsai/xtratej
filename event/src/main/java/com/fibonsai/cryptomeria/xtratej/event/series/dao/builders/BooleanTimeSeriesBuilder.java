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

import com.fibonsai.cryptomeria.xtratej.event.series.dao.*;

import java.util.Arrays;
import java.util.Comparator;

public class BooleanTimeSeriesBuilder extends TimeSeriesBuilder<BooleanTimeSeriesBuilder> {

    private record Element(long timestamp, boolean value) {}

    private Element[] elements = new Element[0];

    public static BooleanTimeSeries trueTs(long timestamp) {
        return new BooleanTimeSeriesBuilder().add(timestamp, true).build();
    }

    public static BooleanTimeSeries falseTs(long timestamp) {
        return new BooleanTimeSeriesBuilder().add(timestamp, false).build();
    }

    public static BooleanTimeSeries[] toSingleArray(long timestamp, boolean bool) {
        return new BooleanTimeSeries[]{ bool ? trueTs(timestamp) : falseTs(timestamp) };
    }

    public BooleanTimeSeriesBuilder add(long timestamp, boolean value) {
        writeLock.lock();
        try {
            Element element = new Element(timestamp, value);
            if (elements.length >= maxSize) {
                Arrays.sort(elements, Comparator.comparingLong(Element::timestamp));
                this.elements[0] = element;
            } else {
                Element[] _elements = new Element[elements.length + 1];
                System.arraycopy(elements, 0, _elements, 0, elements.length);
                _elements[elements.length] = element;
                this.elements = _elements;
            }
        } finally {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public BooleanTimeSeries build() {
        readLock.lock();
        try {
            long[] _timestamps = new long[elements.length];
            boolean[] _values = new boolean[elements.length];
            Arrays.sort(elements, Comparator.comparingLong(Element::timestamp));
            int count = 0;
            for (var element: elements) {
                _timestamps[count] = element.timestamp();
                _values[count] = element.value();
                count++;
            }
            return new BooleanTimeSeries(id, _timestamps, _values);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public BooleanTimeSeriesBuilder from(TimeSeries timeSeries) {
        switch (timeSeries) {
            case BooleanTimeSeries(String id1, long[] timestamps, boolean[] values) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x]);
                    if (id1 != null) setId(id1);
                }
            }
            case DoubleTimeSeries(String id1, long[] timestamps, double[] values) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x] > 0.0);
                    if (id1 != null) setId(id1);
                }
            }
            case Double2TimeSeries(String id1, long[] timestamps, double[] values, double[] _) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x] > 0.0);
                    if (id1 != null) setId(id1);
                }
            }
            case BandTimeSeries(String id1, long[] timestamps, double[] uppers, double[] middles, double[] lowers) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], middles[x] < uppers[x] && middles[x] > lowers[x]);
                    if (id1 != null) setId(id1);
                }
            }
            default -> throw new UnsupportedOperationException("%s not supported".formatted(timeSeries.getClass().getSimpleName()));
        }
        return this;
    }

    @Override
    public BooleanTimeSeriesBuilder merge(TimeSeries... timeSeriesArray) {
        for (var timeSeries: timeSeriesArray) {
            from(timeSeries);
        }
        return this;
    }
}
