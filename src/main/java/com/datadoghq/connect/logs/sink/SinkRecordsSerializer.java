/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkRecordsSerializer {

    private static final Logger log = LoggerFactory.getLogger(SinkRecordsSerializer.class);
    private final JsonConverter jsonConverter;
    private final ByteArrayOutputStream outputStream;
    private final String ddSource;
    private final String ddTags;
    private final String ddHostname;
    private final String ddService;
    private final long maxPayloadSize;

    public SinkRecordsSerializer(String ddSource, String ddTags, String ddHostname, String ddService,
            long maxPayloadSize) {
        this.ddSource = ddSource;
        this.ddTags = ddTags;
        this.ddHostname = ddHostname;
        this.ddService = ddService;
        this.maxPayloadSize = maxPayloadSize;
        this.jsonConverter = new JsonConverter();
        this.outputStream = new ByteArrayOutputStream();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public List<String> serialize(String topic, List<SinkRecord> sinkRecords) throws IOException {
        this.outputStream.reset();
        List<String> output = new ArrayList<String>();
        JsonWriter writer = StartJson(this.outputStream);
        writer.beginArray();

        for (SinkRecord record : sinkRecords) {
            if (record == null || record.value() == null) {
                continue;
            }

            JsonElement recordJSON = recordToJSON(record);
            JsonObject jsonObject = populateMetadata(topic, recordJSON);
            String message = jsonObject.toString();
            if (message.length() + 2 > maxPayloadSize) { // +2 (begin and end of the JSON array)
                log.debug("Single message exeed the maximum payload size");
            }

            writer.flush();
            if (this.outputStream.size() + message.length() + 2 > maxPayloadSize) { // +1 for `,` +1 for `]`
                output.add(FinishJson(this.outputStream, writer));
                writer = StartJson(this.outputStream);
            }
            writer.jsonValue(message);
        }

        writer.flush();
        if (this.outputStream.size() > 0) {
            output.add(FinishJson(this.outputStream, writer));
        }
        writer.close();
        return output;
    }

    private static JsonWriter StartJson(ByteArrayOutputStream outputStream) throws IOException {
        OutputStreamWriter streamWriter = new OutputStreamWriter(outputStream, "UTF-8");
        JsonWriter writer = new JsonWriter(streamWriter);
        writer.beginArray();
        return writer;
    }

    private static String FinishJson(ByteArrayOutputStream outputStream, JsonWriter writer) throws IOException {
        writer.endArray();
        writer.flush();
        String json = outputStream.toString();
        outputStream.reset();
        return json;
    }

    private JsonElement recordToJSON(SinkRecord record) {
        byte[] rawJSONPayload = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String jsonPayload = new String(rawJSONPayload, StandardCharsets.UTF_8);
        return new Gson().fromJson(jsonPayload, JsonElement.class);
    }

    private JsonObject populateMetadata(String topic, JsonElement message) {
        JsonObject content = new JsonObject();
        String tags = "topic:" + topic;
        content.add("message", message);
        content.add("ddsource", new JsonPrimitive(this.ddSource));

        if (this.ddTags != null) {
            tags += "," + this.ddTags;
        }
        content.add("ddtags", new JsonPrimitive(tags));

        if (this.ddHostname != null) {
            content.add("hostname", new JsonPrimitive(this.ddHostname));
        }

        if (this.ddService != null) {
            content.add("service", new JsonPrimitive(this.ddService));
        }

        return content;
    }
}
