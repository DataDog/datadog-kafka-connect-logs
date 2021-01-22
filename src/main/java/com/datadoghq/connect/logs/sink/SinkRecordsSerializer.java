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

public class SinkRecordsSerializer {

    private final JsonConverter jsonConverter;
    private final DatadogLogsSinkConnectorConfig config;
    private final ByteArrayOutputStream outputStream;

    public SinkRecordsSerializer(DatadogLogsSinkConnectorConfig config) {
        this.config = config;
        this.jsonConverter = new JsonConverter();
        this.outputStream = new ByteArrayOutputStream();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public List<String> serialize(String topic, List<SinkRecord> sinkRecords) throws IOException {
        this.outputStream.reset();

        JsonWriter writer = new JsonWriter(new OutputStreamWriter(this.outputStream, "UTF-8"));
        writer.beginArray();
        Gson gson = new Gson();

        for (SinkRecord record : sinkRecords) {
            if (record == null) {
                continue;
            }

            if (record.value() == null) {
                continue;
            }

            JsonElement recordJSON = recordToJSON(record);
            JsonObject message = populateMetadata(topic, recordJSON);
            gson.toJson(message, writer);
            writer.flush();
        }
        writer.endArray();
        writer.close();

        
        List<String> result = new ArrayList<String>();
        result.add(this.outputStream.toString());
        return result;
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
        content.add("ddsource", new JsonPrimitive(config.ddSource));

        if (config.ddTags != null) {
            tags += "," + config.ddTags;
        }
        content.add("ddtags", new JsonPrimitive(tags));

        if (config.ddHostname != null) {
            content.add("hostname", new JsonPrimitive(config.ddHostname));
        }

        if (config.ddService != null) {
            content.add("service", new JsonPrimitive(config.ddService));
        }

        return content;
    }
}
