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

package com.fibonsai.xtratej.adaptor.nats;

import com.fibonsai.xtratej.adaptor.core.Subscriber;
import com.fibonsai.xtratej.adaptor.core.WithParams;
import tools.jackson.databind.JsonNode;

public class NatsSubscriber extends Subscriber implements WithParams {

    private final NatsClient natsClient = new NatsClient(this);

    public NatsSubscriber(String name, String publisher) {
        super(name, publisher);
    }

    @Override
    public Subscriber setParams(JsonNode params) {
        natsClient.setParams(params);
        return this;
    }

    @Override
    public boolean connect() {
        if (natsClient.connect()) {
            natsClient.subscribe();
        }
        return natsClient.isSubscribed();
    }

    @Override
    public boolean disconnect() {
        return natsClient.disconnect();
    }

    @Override
    public boolean isConnected() {
        return natsClient.isSubscribed();
    }
}
