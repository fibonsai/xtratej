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

package com.fibonsai.xtratej.event.series.dao.builders;

import com.fibonsai.xtratej.event.series.dao.*;

import java.util.Arrays;
import java.util.Comparator;

public class DoubleTimeSeriesBuilder extends TimeSeriesBuilder<DoubleTimeSeriesBuilder> {

    private record Element(long timestamp, double value) {}

    private Element[] elements = new Element[0];

    public DoubleTimeSeriesBuilder add(long timestamp, double value) {
        writeLock.lock();
        try {
            Element element = new Element(timestamp, value);
            if (elements.length >= maxSize) {
                Arrays.sort(elements, Comparator.comparingLong(Element::timestamp));
                this.elements[0] = element;
            } else {
                Element[] newElements = new Element[elements.length + 1];
                System.arraycopy(elements, 0, newElements, 0, elements.length);
                newElements[elements.length] = element;
                this.elements = newElements;
            }
        } finally {
            writeLock.unlock();
        }
        return this;
    }

    @Override
    public DoubleTimeSeries build() {
        readLock.lock();
        try {
            long[] _timestamps = new long[elements.length];
            double[] _values = new double[elements.length];
            Arrays.sort(elements, Comparator.comparingLong(Element::timestamp));
            int count = 0;
            for (var element: elements) {
                _timestamps[count] = element.timestamp();
                _values[count] = element.value();
                count++;
            }
            return new DoubleTimeSeries(id, _timestamps, _values);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public DoubleTimeSeriesBuilder from(TimeSeries timeSeries) {
        switch (timeSeries) {
            case DoubleTimeSeries(String id1, long[] timestamps, double[] values) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x]);
                    setId(id1);
                }
            }
            case BarTimeSeries(String id1, long[] timestamps, double[] _, double[] _, double[] _, double[] closes, double[] _) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], closes[x]);
                    setId(id1);
                }
            }
            case BandTimeSeries(String id1, long[] timestamps, double[] uppers, double[] middles, double[] lowers) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], uppers[x] + middles[x] + lowers[x] / 3.0);
                    setId(id1);
                }
            }
            case BooleanTimeSeries(String id1, long[] timestamps, boolean[] values) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x] ? 1.0 : 0.0);
                    setId(id1);
                }
            }
            case Double2TimeSeries(String id1, long[] timestamps, double[] values, double[] _) -> {
                for (int x = 0; x < timestamps.length; x++) {
                    add(timestamps[x], values[x]);
                    setId(id1);
                }
            }
            default -> throw new UnsupportedOperationException("%s not supported".formatted(timeSeries.getClass().getSimpleName()));
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
