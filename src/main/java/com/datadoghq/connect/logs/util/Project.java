/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.logs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class Project {
    private static final Logger log = LoggerFactory.getLogger(Project.class);
    private static final String PATH = "/datadog-kafka-connect-logs.properties";
    private static String version = "unknown";

    static {
        try (InputStream stream = Project.class.getResourceAsStream(PATH)) {
            Properties properties = new Properties();
            properties.load(stream);
            version = properties.getProperty("version", version).trim();
        } catch (Exception e) {
            log.warn("Error while loading version: ", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}
