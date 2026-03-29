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

import com.fibonsai.directflux.DirectFlux;
import com.fibonsai.xtratej.adapter.core.Adapter;
import com.fibonsai.xtratej.adapter.core.WithParams;
import com.fibonsai.xtratej.event.series.dao.*;
import io.nats.client.*;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.fibonsai.xtratej.adapter.nats.NatsClient.NatsKey.*;

public class NatsClient implements Adapter, WithParams {

    private final DirectFlux<TimeSeries> caller;

    public enum NatsKey {
        NATS_CREDS("nats-creds"),
        SERVERS("servers"),
        MAX_RECONNECTS("max-reconnects"),
        MAX_MESSAGES_INOUTGOING_QUEUE("max-messages-outgoing-queue"),
        TOPICS("topics"),
        ;

        private final String key;

        NatsKey(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    public static final List<Class<? extends TimeSeries>> CLASSES_SUPPORTED = List.of(DoubleTimeSeries.class, BarTimeSeries.class, BooleanTimeSeries.class);

    private static final Logger log = LoggerFactory.getLogger(NatsClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Options natsOptions = Options.builder().build();
    private @Nullable Connection connection;
    private @Nullable Dispatcher dispatcher;

    private boolean connected = false;
    private boolean subscribed = false;

    private final List<String> topics = new ArrayList<>();

    private Runnable onConnect = () -> {};
    private Runnable onDisconnect = () -> {};

    public NatsClient(DirectFlux<TimeSeries> caller) {
        this.caller = caller;
    }

    private static TimeSeries decode(byte[] data, @Nullable Headers headers) {
        String msg = new String(data, StandardCharsets.UTF_8);
        String className = headers != null ? headers.getFirst("class") : Object.class.getSimpleName();
        TimeSeries timeSeries = null;

        if (className != null) {
            for (var clazz : CLASSES_SUPPORTED) {
                if (clazz.getSimpleName().equals(className)) {
                    timeSeries = decode(clazz, msg);
                    break;
                }
            }
        }

        return timeSeries == null ? EmptyTimeSeries.INSTANCE : timeSeries;
    }

    private static TimeSeries decode(Class<? extends TimeSeries> clazz, String msg) {
        return MAPPER.readValue(msg, clazz);
    }

    private static byte[] encode(TimeSeries timeSeries) {
        return MAPPER.writeValueAsBytes(timeSeries);
    }

    @Override
    public NatsClient setParams(JsonNode params) {
        String natsCreds = null;
        Options.Builder natsOptionsBuilder = Options.builder();
        for (var param: params) {
            if (param.hasNonNull(NATS_CREDS.key()) && param.get(NATS_CREDS.key()).isString()) {
                natsCreds = param.get(NATS_CREDS.key()).asString();
            }
            if (param.hasNonNull(SERVERS.key()) && param.get(SERVERS.key()).isArray()) {
                for (var server: param.get(SERVERS.key())) {
                    natsOptionsBuilder.server(server.asString());
                }
            }
            if (param.hasNonNull(MAX_RECONNECTS.key()) && param.get(MAX_RECONNECTS.key()).isInt()) {
                natsOptionsBuilder.maxReconnects(param.get(MAX_RECONNECTS.key()).asInt());
            }
            if (param.hasNonNull(MAX_MESSAGES_INOUTGOING_QUEUE.key()) && param.get(MAX_MESSAGES_INOUTGOING_QUEUE.key()).isInt()) {
                natsOptionsBuilder.maxMessagesInOutgoingQueue(param.get(MAX_MESSAGES_INOUTGOING_QUEUE.key()).asInt());
            }
            if (param.hasNonNull(TOPICS.key()) && param.get(TOPICS.key()).isArray()) {
                for (var topic: param.get(TOPICS.key())) {
                    topics.add(topic.asString());
                }
            }
        }
        if (natsCreds == null) natsCreds = System.getenv("NATS_CREDS");
        if (natsCreds != null) {
            AuthHandler authHandler = Nats.credentials(natsCreds);
            natsOptionsBuilder.authHandler(authHandler);
        }

        natsOptions = natsOptionsBuilder.build();
        return this;
    }

    public void publish(String name, TimeSeries timeSeries, Headers headers) {
        if (isConnected()) {
            connection.publish(name, headers, encode(timeSeries));
        }
    }

    public void subscribe() {
        if (isConnected()) {
            this.dispatcher = connection.createDispatcher(handler());
            this.subscribed = topics.stream().map(dispatcher::subscribe).allMatch(Consumer::isActive);
        }
    }

    public NatsClient onConnect(Runnable runnable) {
        this.onConnect = runnable;
        return this;
    }

    public NatsClient onDisconnect(Runnable runnable) {
        this.onDisconnect = runnable;
        return this;
    }

    @Override
    public boolean connect() {
        try {
            connection = Objects.requireNonNull(Nats.connectReconnectOnConnect(natsOptions));
            connected = connection.getStatus() == Connection.Status.CONNECTED;
        } catch (InterruptedException | IOException e) {
            log.error(e.getMessage(), e);
        } catch (NullPointerException e) {
            log.error("not connected. aborting.");
        }
        try {
            onConnect.run();
        } catch (Exception e) {
            log.error("onConnect error", e);
        }
        return connected;
    }

    @Override
    public boolean disconnect() {
        if (connection != null) {
            try {
                if (isSubscribed()) {
                    CompletableFuture<Boolean> drained = dispatcher.drain(Duration.ofSeconds(10));
                    drained.get();
                    subscribed = false;
                }
                connection.close();
                connected = false;
            } catch (ExecutionException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            try {
                onDisconnect.run();
            } catch (Exception e) {
                log.error("onDisconnect error", e);
            }
        }
        return !connected;
    }

    @Override
    public boolean isConnected() {
        return connection != null && connected;
    }

    public boolean isSubscribed() {
        return dispatcher != null && subscribed;
    }

    private MessageHandler handler() {
        return raw -> {
            byte[] data = raw.getData();
            Headers headers = raw.getHeaders();

            TimeSeries timeSeries = decode(data, headers);

            if (timeSeries != EmptyTimeSeries.INSTANCE) {
                if (log.isDebugEnabled()) {
                    log.debug(">>>>>>> [{}] SEND {}", timeSeries.timestamp(), timeSeries);
                }
                caller.emitNext(timeSeries);
            } else {
                log.warn("header `class` NOT defined or its value IS NOT supported");
            }
        };
    }
}
