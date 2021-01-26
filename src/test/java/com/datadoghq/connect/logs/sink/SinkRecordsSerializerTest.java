/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class SinkRecordsSerializerTest {
    @Test
    public void simpleSerialize() throws IOException {
        SinkRecordsSerializer serializer = new SinkRecordsSerializer("mySource", "myTags", "myHostname", "myService",
                1420);
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord("someTopic", 0, null, "someKey", null, "someValue1", 0));

        List<String> res = serializer.serialize("someTopic", records);
        Assert.assertEquals(1, res.size());
        System.out.println(res.get(0));
        Assert.assertEquals(
                "[{\"message\":\"someValue1\",\"ddsource\":\"mySource\",\"ddtags\":\"topic:someTopic,myTags\",\"hostname\":\"myHostname\",\"service\":\"myService\"}]",
                res.get(0));
    }

    @Test
    public void serializePayloadTooBig() throws IOException {
        List<SinkRecord> records = new ArrayList<SinkRecord>();
        for (int i = 0; i < 3; i++) {
            records.add(new SinkRecord("someTopic" + i, 0, null, "someKey" + i, null, "someValue" + i, 0));
        }

        List<String> payloads = serializeSinkRecord(records, 500);
        Assert.assertEquals(1, payloads.size());
        String payload = payloads.get(0);
        int payloadSize = payload.length();

        for (int maxPayloadSize = payloadSize / 2; maxPayloadSize < payloadSize; maxPayloadSize++) {
            List<String> payloadsWithMaxPayloadSize = serializeSinkRecord(records, maxPayloadSize);
            for (String r : payloadsWithMaxPayloadSize) {
                Assert.assertTrue(r.length() <= maxPayloadSize);
            }
            AssertJsonEquals(payload, payloadsWithMaxPayloadSize);
        }
    }

    private static List<String> serializeSinkRecord(List<SinkRecord> records, int payloadSize) throws IOException {
        SinkRecordsSerializer serializer = new SinkRecordsSerializer("mySource", "myTags", "myHostname", "myService",
                payloadSize);
        return serializer.serialize("someTopic", records);
    }

    public class SinkRecordMock {
        public String message;
        public String ddsource;
        public String ddtags;
        public String hostname;
        public String service;
    }

    private void AssertJsonEquals(String json, List<String> jsons) {
        Gson gson = new Gson();

        ArrayList<SinkRecordMock> sinkRecords = new ArrayList<SinkRecordMock>();
        for (String j : jsons) {
            Type listType = new TypeToken<ArrayList<SinkRecordMock>>() {
            }.getType();
            sinkRecords.addAll(gson.fromJson(j, listType));
        }
        Assert.assertEquals(json, gson.toJson(sinkRecords));
    }
}
