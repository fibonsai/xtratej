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

package com.fibonsai.xtratej.adapter.nats;

import com.fibonsai.xtratej.adapter.core.Publisher;
import com.fibonsai.xtratej.adapter.core.WithParams;
import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import com.fibonsai.xtratej.event.series.dao.TradingSignal;
import io.nats.client.impl.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.util.function.Consumer;

public class NatsPublisher extends Publisher implements WithParams {

    private static final Logger log = LoggerFactory.getLogger(NatsPublisher.class);

    private final NatsClient natsClient = new NatsClient(this);
    private final Headers headers = new Headers().add("class", TradingSignal.class.getSimpleName());

    public NatsPublisher(String name) {
        super(name);
        subscribe(publishToNats());
    }

    @Override
    public NatsPublisher setParams(JsonNode params) {
        natsClient.setParams(params);
        return this;
    }

    private Consumer<TimeSeries> publishToNats() {
        return timeSeries -> {
            if (isConnected()) {
                natsClient.publish(name(), timeSeries, headers);
            } else {
                log.error("not connected. Ignoring {}", timeSeries);
            }
        };
    }

    @Override
    public boolean connect() {
        return natsClient.connect();
    }

    @Override
    public boolean disconnect() {
        return natsClient.disconnect();
    }

    @Override
    public boolean isConnected() {
        return natsClient.isConnected();
    }
}
