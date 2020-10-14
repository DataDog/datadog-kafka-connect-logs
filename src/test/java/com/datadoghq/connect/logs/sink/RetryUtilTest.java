package com.datadoghq.connect.logs.sink;

import com.datadoghq.connect.logs.sink.util.RestHelper;
import com.datadoghq.connect.logs.util.RetryUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RetryUtilTest {
    private RetryUtil retryUtil;

    @Before
    public void setUp() throws Exception {
        retryUtil = new RetryUtil();
    }

    @Test
    public void computeRetryBackoffForValidRanges() {
        assertComputeRetryInRange(10, 10L);
        assertComputeRetryInRange(10, 100L);
        assertComputeRetryInRange(10, 1000L);
        assertComputeRetryInRange(100, 1000L);
    }

    @Test
    public void computeRetryBackoffForNegativeRetryTimes() {
        assertComputeRetryInRange(1, -100L);
        assertComputeRetryInRange(10, -100L);
        assertComputeRetryInRange(100, -100L);
    }

    @Test
    public void computeNonRandomRetryTimes() {
        assertEquals(100L, retryUtil.computeRetryWaitTimeInMillis(0, 100L));
        assertEquals(200L, retryUtil.computeRetryWaitTimeInMillis(1, 100L));
        assertEquals(400L, retryUtil.computeRetryWaitTimeInMillis(2, 100L));
        assertEquals(800L, retryUtil.computeRetryWaitTimeInMillis(3, 100L));
        assertEquals(1600L, retryUtil.computeRetryWaitTimeInMillis(4, 100L));
        assertEquals(3200L, retryUtil.computeRetryWaitTimeInMillis(5, 100L));
    }

    protected void assertComputeRetryInRange(int retryAttempts, long retryBackoffMs) {
        for (int i = 0; i != 20; ++i) {
            for (int retries = 0; retries <= retryAttempts; ++retries) {
                long maxResult = retryUtil.computeRetryWaitTimeInMillis(retries, retryBackoffMs);
                long result = retryUtil.computeRandomRetryWaitTimeInMillis(retries, retryBackoffMs);
                if (retryBackoffMs < 0) {
                    assertEquals(0, result);
                } else {
                    assertTrue(result >= 0L);
                    assertTrue(result <= maxResult);
                }
            }
        }
    }
}

