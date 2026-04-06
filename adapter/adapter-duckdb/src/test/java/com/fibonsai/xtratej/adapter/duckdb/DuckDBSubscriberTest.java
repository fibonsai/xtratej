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

package com.fibonsai.xtratej.adapter.duckdb;

import com.fibonsai.xtratej.adapter.core.decoders.DecoderFactory;
import com.fibonsai.xtratej.event.series.dao.BarTimeSeries;
import com.fibonsai.xtratej.event.series.dao.OrderTimeSeries;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.util.concurrent.AtomicDouble;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class DuckDBSubscriberTest {

    private static final Logger log = LoggerFactory.getLogger(DuckDBSubscriberTest.class);

    private static final String TRADE_S3URL = "s3://my-bucket/trades.parquet";
    private static final String CANDLE_S3URL = "s3://my-bucket/ohlcv.parquet";

    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    private final ObjectNode newParams = jsonNodeFactory.objectNode();

    private DuckDBSubscriber subscriber;

    @Container
    private static final MinIOContainer container = new MinIOContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z");

    @BeforeAll
    public static void init() {
        String uploadedTrades = uploadFileToMinIO("trades.parquet");
        String uploadedOhlcv = uploadFileToMinIO("ohlcv.parquet");
        assertFalse(uploadedTrades.isBlank());
        assertFalse(uploadedOhlcv.isBlank());
        log.info(uploadedTrades);
        log.info(uploadedOhlcv);
    }

    @BeforeEach
    public void beforeEach() {
        ObjectNode otherProperties = jsonNodeFactory.objectNode();
        otherProperties.put("s3_use_ssl", false);
        URI endpointUri = URI.create(container.getS3URL());
        otherProperties.put("s3_endpoint", "%s:%s".formatted(endpointUri.getHost(), endpointUri.getPort()));
        otherProperties.put("s3_url_style", "path");
        otherProperties.put("s3_region", "us-east-1");

        newParams.put(DuckDBClient.DuckDBKey.ACCOUNT.key(), container.getUserName());
        newParams.put(DuckDBClient.DuckDBKey.SECRET.key(), container.getPassword());
        newParams.set(DuckDBClient.DuckDBKey.OTHER_PROPERTIES.key(), otherProperties);

        subscriber = new DuckDBSubscriber("test", "test");
    }

    @AfterEach
    public void afterEach() {
        if (subscriber != null && subscriber.isConnected()) {
            subscriber.disconnect();
            subscriber = null;
        }
    }

    private static boolean bucketExists(S3Client s3Client, String bucketName) {
        try {
            s3Client.headBucket(request -> request.bucket(bucketName));
            return true;
        }
        catch (NoSuchBucketException exception) {
            return false;
        }
    }

    private static String uploadFileToMinIO(String parquetFileName) {
        try {
            var resource = DuckDBSubscriberTest.class.getClassLoader().getResource(parquetFileName);
            String parquetFilePath = Paths.get(Objects.requireNonNull(resource).toURI()).toAbsolutePath().toString();
            String bucketName = "my-bucket";
            AwsCredentials credentials = AwsBasicCredentials.create(container.getUserName(), container.getPassword());
            try (S3Client s3Client = S3Client
                    .builder()
                    .endpointOverride(URI.create(container.getS3URL()))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .forcePathStyle(true)
                    .build()) {
                if (!bucketExists(s3Client, bucketName)) {
                    s3Client.createBucket(request -> request.bucket(bucketName));
                }
                File file = new File(parquetFilePath);
                s3Client.putObject(request ->
                    request.bucket(bucketName)
                            .key(file.getName())
                            .ifNoneMatch("*")
                , file.toPath());

                ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .build();
                ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);

                List<S3Object> contents = listObjectsV2Response.contents();
                S3Object s3Object = contents.getFirst();
                return "%s s3://%s/%s: %s bytes".formatted(s3Object.lastModified(), bucketName, s3Object.key(), s3Object.size());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        } catch (URISyntaxException e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

    @Test
    void testConnectAndSubscribe() throws Exception {
        newParams.put(DuckDBClient.DuckDBKey.SOURCE_DATA.key(), DecoderFactory.FT_DATA_TRADE.name());
        String query = "SELECT * FROM read_parquet('%s') LIMIT 1".formatted(TRADE_S3URL);
        newParams.put(DuckDBClient.DuckDBKey.QUERY.key(), query);
        subscriber.setParams(newParams);
        CountDownLatch latch = new CountDownLatch(2);
        subscriber.onConnect(latch::countDown);
        subscriber.onSubscribe(latch::countDown);
        subscriber.subscribe(_ -> {});
        boolean connected = subscriber.connect();
        assertTrue(connected, "Should connect successfully");
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    void testReceiveTradeFromS3() throws Exception {
        newParams.put(DuckDBClient.DuckDBKey.SOURCE_DATA.key(), DecoderFactory.FT_DATA_TRADE.name());
        String query = "SELECT * FROM read_parquet('%s') LIMIT 1".formatted(TRADE_S3URL);
        newParams.put(DuckDBClient.DuckDBKey.QUERY.key(), query);
        subscriber.setParams(newParams);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean received = new AtomicBoolean(false);
        AtomicLong timestamp = new AtomicLong(0L);
        AtomicReference<String> id = new AtomicReference<>("");
        AtomicReference<OrderTimeSeries.BidAskSide> side = new AtomicReference<>(OrderTimeSeries.BidAskSide.UNDEF);
        AtomicDouble price = new AtomicDouble(0.0);
        AtomicDouble executedAmount = new AtomicDouble(0.0);
        subscriber.subscribe(ts -> {
            received.compareAndSet(false, ts instanceof OrderTimeSeries);
            if (ts instanceof OrderTimeSeries orderTimeSeries && orderTimeSeries.size() == 1) {
                timestamp.set(orderTimeSeries.timestamps()[0]);
                side.set(orderTimeSeries.sides()[0]);
                id.set(orderTimeSeries.orderIds()[0]);
                price.set(orderTimeSeries.prices()[0]);
                executedAmount.set(orderTimeSeries.executedAmounts()[0]);
            }
            latch.countDown();
        });
        boolean connected = subscriber.connect();
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(connected, "Should connect successfully");
        assertTrue(received.get(), "Should receive message within timeout");
        assertEquals(1775260800070L, timestamp.get(), "timestamp not match");
        assertEquals("3924721793", id.get(), "order ID not match");
        assertEquals(OrderTimeSeries.BidAskSide.ASK, side.get(), "side not match");
        assertEquals(66964.29, price.get(), "price not match");
        assertEquals(0.00009, executedAmount.get(), "amount not match");
    }

    @Test
    void testReceiveCandleFromS3() throws Exception {
        newParams.put(DuckDBClient.DuckDBKey.SOURCE_DATA.key(), DecoderFactory.FT_DATA_CANDLESTICK.name());
        String query = "SELECT * FROM read_parquet('%s') LIMIT 1".formatted(CANDLE_S3URL);
        newParams.put(DuckDBClient.DuckDBKey.QUERY.key(), query);
        subscriber.setParams(newParams);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean received = new AtomicBoolean(false);
        AtomicLong timestamp = new AtomicLong(0L);
        AtomicDouble open = new AtomicDouble(0.0);
        AtomicDouble high = new AtomicDouble(0.0);
        AtomicDouble low = new AtomicDouble(0.0);
        AtomicDouble close = new AtomicDouble(0.0);
        AtomicDouble volume = new AtomicDouble(0.0);
        subscriber.subscribe(ts -> {
            received.compareAndSet(false, ts instanceof BarTimeSeries);
            if (ts instanceof BarTimeSeries barTimeSeries && barTimeSeries.size() == 1) {
                timestamp.set(barTimeSeries.timestamps()[0]);
                open.set(barTimeSeries.opens()[0]);
                high.set(barTimeSeries.highs()[0]);
                low.set(barTimeSeries.lows()[0]);
                close.set(barTimeSeries.closes()[0]);
                volume.set(barTimeSeries.volumes()[0]);
            }
            latch.countDown();
        });
        boolean connected = subscriber.connect();
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(connected, "Should connect successfully");
        assertTrue(received.get(), "Should receive message within timeout");
        assertEquals(1775250000000L, timestamp.get(), "timestamp not match");
        assertEquals(66964.29, open.get(), "open not match");
        assertEquals(66964.3, high.get(), "high not match");
        assertEquals(66900.79, low.get(), "low not match");
        assertEquals(66900.8, close.get(), "close not match");
        assertEquals(14.16535, volume.get(), "volume not match");
    }

    @Test
    void testDisconnect() throws Exception {
        subscriber.setParams(newParams);
        CountDownLatch latch = new CountDownLatch(1);
        subscriber.onConnect(latch::countDown);
        boolean connected = subscriber.connect();
        assertTrue(connected, "Should connect successfully");
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        boolean disconnected = subscriber.disconnect();
        assertTrue(disconnected, "Should disconnect successfully");
    }
}
