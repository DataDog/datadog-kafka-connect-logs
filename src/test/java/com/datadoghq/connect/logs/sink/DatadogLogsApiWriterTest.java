/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import com.datadoghq.connect.logs.sink.util.RequestInfo;
import com.datadoghq.connect.logs.sink.util.RestHelper;
import com.datadoghq.connect.logs.util.Project;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatadogLogsApiWriterTest {
    private static String apiKey = "API_KEY";
    private Map<String, String> props;
    private List<SinkRecord> records;
    private RestHelper restHelper;

    @Before
    public void setUp() throws Exception {
        records = new ArrayList<>();
        props = new HashMap<>();
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, apiKey);
        props.put(DatadogLogsSinkConnectorConfig.DD_URL, "localhost:8080");
        restHelper = new RestHelper();
        restHelper.start();
    }

    @After
    public void tearDown() throws Exception {
        restHelper.stop();
        restHelper.flushCapturedRequests();
    }

    @Test
    public void writer_givenConfigs_sendsPOSTToURL() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));
        writer.write(records);

        Assert.assertEquals(1, restHelper.getCapturedRequests().size());
        RequestInfo request = restHelper.getCapturedRequests().get(0);
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("/api/v2/logs", request.getUrl());
        Assert.assertTrue(request.getHeaders().contains("Content-Type:application/json"));
        Assert.assertTrue(request.getHeaders().contains("Content-Encoding:gzip"));
        Assert.assertTrue(request.getHeaders().contains("DD-API-KEY:" + apiKey));
        Assert.assertTrue(request.getHeaders().contains("DD-EVP-ORIGIN:datadog-kafka-connect-logs"));
        Assert.assertTrue(request.getHeaders().contains("DD-EVP-ORIGIN-VERSION:" + Project.getVersion()));
        Assert.assertTrue(request.getHeaders().contains("User-Agent:datadog-kafka-connect-logs/" + Project.getVersion()));
    }

    @Test
    public void writer_handles_bigDecimal() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        Schema schema = Decimal.schema(2);
        BigDecimal value = new BigDecimal(new BigInteger("156"), 2);

        records.add(new SinkRecord("someTopic", 0, null, "someKey", schema, value, 0));
        writer.write(records);

        Assert.assertEquals(1, restHelper.getCapturedRequests().size());
        RequestInfo request = restHelper.getCapturedRequests().get(0);
        Assert.assertEquals("[{\"message\":1.56,\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic\"}]", request.getBody());
    }

    @Test
    public void writer_batchAtMax_shouldSendBatched() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 2, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue2", 0));
        writer.write(records);

        Assert.assertEquals(1, restHelper.getCapturedRequests().size());

        RequestInfo request = restHelper.getCapturedRequests().get(0);
        Assert.assertEquals("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic\"},{\"message\":\"someValue2\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic\"}]", request.getBody());
    }

    @Test
    public void writer_batchAboveMax_shouldSendSeparate() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 1, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue2", 0));
        writer.write(records);

        Assert.assertEquals(2, restHelper.getCapturedRequests().size());

        RequestInfo request1 = restHelper.getCapturedRequests().get(0);
        RequestInfo request2 = restHelper.getCapturedRequests().get(1);

        Set<String> requestBodySetActual = new HashSet<>();
        requestBodySetActual.add(request1.getBody());
        requestBodySetActual.add(request2.getBody());
        Set<String> requestBodySetExpected = new HashSet<>();
        requestBodySetExpected.add("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic\"}]");
        requestBodySetExpected.add("[{\"message\":\"someValue2\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic\"}]");
        Assert.assertEquals(requestBodySetExpected, requestBodySetActual);
    }

    @Test
    public void writer_readingMultipleTopics_shouldBatchSeparate() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 2, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic1", 0, null, "someKey", null, "someValue1", 0));
        records.add(new SinkRecord("someTopic2", 0, null, "someKey", null, "someValue2", 0));
        writer.write(records);

        Assert.assertEquals(2, restHelper.getCapturedRequests().size());

        RequestInfo request1 = restHelper.getCapturedRequests().get(0);
        RequestInfo request2 = restHelper.getCapturedRequests().get(1);

        Set<String> requestBodySetActual = new HashSet<>();
        requestBodySetActual.add(request1.getBody());
        requestBodySetActual.add(request2.getBody());
        Set<String> requestBodySetExpected = new HashSet<>();
        requestBodySetExpected.add("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic1\"}]");
        requestBodySetExpected.add("[{\"message\":\"someValue2\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic2\"}]");
        Assert.assertEquals(requestBodySetExpected, requestBodySetActual);
    }

    @Test(expected = IOException.class)
    public void writer_IOException_for_status_429() throws Exception {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        restHelper.setHttpStatusCode(429);
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));
        writer.write(records);
    }

    @Test
    public void metadata_asOneBatch_shouldPopulatePerBatch() throws IOException {
        props.put(DatadogLogsSinkConnectorConfig.DD_TAGS, "team:agent-core, author:berzan");
        props.put(DatadogLogsSinkConnectorConfig.DD_HOSTNAME, "test-host");
        props.put(DatadogLogsSinkConnectorConfig.DD_SERVICE, "test-service");

        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue2", 0));
        writer.write(records);

        RequestInfo request = restHelper.getCapturedRequests().get(0);

        Assert.assertEquals("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic,team:agent-core,author:berzan\",\"hostname\":\"test-host\",\"service\":\"test-service\"},{\"message\":\"someValue2\",\"ddsource\":\"kafka-connect\",\"ddtags\":\"topic:someTopic,team:agent-core,author:berzan\",\"hostname\":\"test-host\",\"service\":\"test-service\"}]", request.getBody());
    }

    @Test
    public void writer_withUseRecordTimeStampEnabled_shouldPopulateRecordTimestamp() throws IOException {
        props.put(DatadogLogsSinkConnectorConfig.ADD_PUBLISHED_DATE, "true");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 2, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);


        long recordTime = 1713974401224L;

        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0, recordTime, TimestampType.CREATE_TIME));
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue2", 0, recordTime, TimestampType.CREATE_TIME));
        writer.write(records);

        Assert.assertEquals(1, restHelper.getCapturedRequests().size());

        RequestInfo request = restHelper.getCapturedRequests().get(0);
        System.out.println(request.getBody());
        Assert.assertEquals("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"published_date\":1713974401224,\"ddtags\":\"topic:someTopic\"},{\"message\":\"someValue2\",\"ddsource\":\"kafka-connect\",\"published_date\":1713974401224,\"ddtags\":\"topic:someTopic\"}]", request.getBody());
    }

    @Test
    public void writer_parse_record_headers_enabled() throws IOException {
        props.put(DatadogLogsSinkConnectorConfig.PARSE_RECORD_HEADERS, "true");
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 2, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);


        Schema keySchema = Schema.INT32_SCHEMA;
        Schema valueSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Integer key = 123;
        Struct value = new Struct(valueSchema)
                .put("field1", "value1")
                .put("field2", 456);

        Headers headers = new ConnectHeaders();
        headers.addString("headerKey", "headerValue");

        long recordTime = 1713974401224L;

        SinkRecord sinkRecord = new SinkRecord("topicName", 0, keySchema, key, valueSchema, value,
                100L, recordTime, null, headers);

        records.add(sinkRecord);
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null,
                "someValue1", 0, recordTime, TimestampType.CREATE_TIME));
        writer.write(records);

        Assert.assertEquals(2, restHelper.getCapturedRequests().size());

        RequestInfo requestWithHeaders = restHelper.getCapturedRequests().get(0);
        RequestInfo requestWithoutHeaders = restHelper.getCapturedRequests().get(1);

        Set<String> requestBodySetActual = new HashSet<>();
        requestBodySetActual.add(requestWithHeaders.getBody());
        requestBodySetActual.add(requestWithoutHeaders.getBody());
        Set<String> requestBodySetExpected = new HashSet<>();
        requestBodySetExpected.add("[{\"message\":{\"field1\":\"value1\",\"field2\":456},\"ddsource\":\"kafka-connect\",\"kafkaheaders\":{\"headerKey\":\"headerValue\"},\"ddtags\":\"topic:topicName\"}]");
        requestBodySetExpected.add("[{\"message\":\"someValue1\",\"ddsource\":\"kafka-connect\",\"kafkaheaders\":{},\"ddtags\":\"topic:someTopic\"}]");
        Assert.assertEquals(requestBodySetExpected, requestBodySetActual);
        props.remove(DatadogLogsSinkConnectorConfig.PARSE_RECORD_HEADERS);
    }

    // --- byte-size batch splitting tests ---

    private String generatePayload(int targetBytes) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < targetBytes) {
            sb.append("abcdefghij");
        }
        return sb.substring(0, targetBytes);
    }

    @Test
    public void writer_byteSizeSplit_producesMultipleRequests() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        String largePayload = generatePayload(2_000_000);
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 1));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 2));
        writer.write(records);

        Assert.assertTrue("Expected multiple HTTP requests from byte-size splitting",
                restHelper.getCapturedRequests().size() > 1);
    }

    @Test
    public void writer_byteSizeSplit_eachBatchIsValidJson() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        String largePayload = generatePayload(2_000_000);
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 1));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, largePayload, 2));
        writer.write(records);

        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
        for (RequestInfo req : restHelper.getCapturedRequests()) {
            com.google.gson.JsonElement parsed = parser.parse(req.getBody());
            Assert.assertTrue("Batch must be a JSON array", parsed.isJsonArray());
            Assert.assertTrue("Batch must not be empty", parsed.getAsJsonArray().size() > 0);
        }
    }

    @Test
    public void writer_byteSizeSplit_exactlyTwoBatchesAtBoundary() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        int perMessageTarget = DatadogLogsApiWriter.MAXIMUM_BATCH_BYTES / 2 - 100;
        String payload = generatePayload(perMessageTarget);
        records.add(new SinkRecord("someTopic", 0, null, "key", null, payload, 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, payload, 1));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, payload, 2));
        writer.write(records);

        Assert.assertEquals("Two messages fit in one batch, third forces a second",
                2, restHelper.getCapturedRequests().size());
    }

    @Test
    public void writer_oversizedSingleMessage_isDroppedButOthersSent() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        String oversized = generatePayload(DatadogLogsApiWriter.MAXIMUM_BATCH_BYTES + 1000);
        records.add(new SinkRecord("someTopic", 0, null, "key", null, oversized, 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, "normalMessage", 1));
        writer.write(records);

        Assert.assertEquals("One request for the normal message", 1, restHelper.getCapturedRequests().size());
        Assert.assertTrue("Normal message should be present",
                restHelper.getCapturedRequests().get(0).getBody().contains("normalMessage"));
    }

    @Test
    public void writer_nullValueRecords_dontCorruptBatch() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        records.add(new SinkRecord("someTopic", 0, null, "key", null, "valid1", 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, null, 1));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, "valid2", 2));
        writer.write(records);

        Assert.assertEquals(1, restHelper.getCapturedRequests().size());
        String body = restHelper.getCapturedRequests().get(0).getBody();
        com.google.gson.JsonElement parsed = new com.google.gson.JsonParser().parse(body);
        Assert.assertTrue(parsed.isJsonArray());
        Assert.assertEquals("Only non-null-value records in batch", 2, parsed.getAsJsonArray().size());
    }

    @Test
    public void writer_cjkPayload_splitsOnByteCountNotCharCount() throws IOException {
        DatadogLogsSinkConnectorConfig config = new DatadogLogsSinkConnectorConfig(false, 500, props);
        DatadogLogsApiWriter writer = new DatadogLogsApiWriter(config);

        // CJK chars: 1 char = 3 UTF-8 bytes.
        // At 1M chars: .length() = 1M, utf8 bytes ≈ 3M.
        // Each serialized message ≈ 3M bytes (fits in one batch alone).
        // Two together ≈ 6M bytes (exceeds 4.5M, must split).
        // But by .length(), two messages ≈ 2M chars (would NOT split).
        int charCount = 1_000_000;
        StringBuilder sb = new StringBuilder(charCount);
        for (int i = 0; i < charCount; i++) {
            sb.append('\u4e16'); // 世 — 3 bytes in UTF-8
        }
        String cjkPayload = sb.toString();

        Assert.assertEquals("Payload char length", charCount, cjkPayload.length());
        Assert.assertEquals("Payload byte length is 3x char length",
                charCount * 3, DatadogLogsApiWriter.utf8ByteLength(cjkPayload));

        records.add(new SinkRecord("someTopic", 0, null, "key", null, cjkPayload, 0));
        records.add(new SinkRecord("someTopic", 0, null, "key", null, cjkPayload, 1));
        writer.write(records);

        Assert.assertEquals("CJK messages must split despite fitting by char count",
                2, restHelper.getCapturedRequests().size());
    }

    // --- utf8ByteLength tests ---

    @Test
    public void utf8ByteLength_ascii_matchesGetBytes() {
        String ascii = "Hello, world! 1234567890";
        Assert.assertEquals(ascii.getBytes(StandardCharsets.UTF_8).length,
                DatadogLogsApiWriter.utf8ByteLength(ascii));
    }

    @Test
    public void utf8ByteLength_multibyte_matchesGetBytes() {
        String mixed = "Hello \u00e9\u00fc\u00f1 \u4e16\u754c \uD83D\uDE00";
        Assert.assertEquals(mixed.getBytes(StandardCharsets.UTF_8).length,
                DatadogLogsApiWriter.utf8ByteLength(mixed));
    }

    @Test
    public void utf8ByteLength_empty_returnsZero() {
        Assert.assertEquals(0, DatadogLogsApiWriter.utf8ByteLength(""));
    }
}
