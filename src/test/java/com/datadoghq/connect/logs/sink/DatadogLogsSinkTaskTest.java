/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DatadogLogsSinkTaskTest extends EasyMockSupport {

    @Test
    public void putTask_onRetry_shouldThrowExceptions() throws IOException {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        Set<SinkRecord> records = Collections.singleton(new SinkRecord(
                "stub",
                0,
                null,
                null,
                null,
                "someVal",
                0
        ));

        final DatadogLogsApiWriter mockWriter = createMock(DatadogLogsApiWriter.class);
        SinkTaskContext ctx = createMock(SinkTaskContext.class);

        mockWriter.write(records);
        expectLastCall().andThrow(new IOException()).times(1 + maxRetries);

        ctx.timeout(retryBackoffMs);
        expectLastCall().times(maxRetries);

        DatadogLogsSinkTask task = new DatadogLogsSinkTask() {
            @Override
            protected void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);

        Map<String, String> props = new HashMap<>();
        props.put(DatadogLogsSinkConnectorConfig.DD_API_KEY, "123");
        props.put(DatadogLogsSinkConnectorConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(DatadogLogsSinkConnectorConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
        task.start(props);

        replayAll();

        try {
            task.put(records);
            fail("Retriable exception expected.");
        } catch (RetriableException expected) {}

        try {
            task.put(records);
            fail("Retriable exception expected.");
        } catch (RetriableException expected) {}

        try {
            task.put(records);
            fail();
        } catch (RetriableException e) {
            fail("Non-retriable exception expected.");
        } catch (ConnectException expected) {
            assertEquals(IOException.class, expected.getCause().getClass());
        }

        verifyAll();
    }
}
