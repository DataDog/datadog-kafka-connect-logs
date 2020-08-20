/*
Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.connect.datadog.logs.sink.util;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class RestHelper extends HttpServlet {

    private Server server;
    private final List<RequestInfo> capturedRequests = new ArrayList<RequestInfo>();
    public static final String apiKey = "test";

    public void start() throws Exception {
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        ServletContextHandler handler = new ServletContextHandler();
        ServletHolder testServ = new ServletHolder("test", this);
        handler.addServlet(testServ,"/v1/input/" + apiKey);

        server.setHandler(handler);
        connector.setPort(1);
        server.setConnectors(new Connector[]{connector});

        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        capturedRequests.add(getRequestInfo(request));

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("{ \"status\": \"ok\"}");
    }

    private RequestInfo getRequestInfo(HttpServletRequest request) throws IOException {
        RequestInfo requestInfo = new RequestInfo();

        // Read from request
        byte[] buffer = new byte[1024];
        InputStream is = request.getInputStream();
        GZIPInputStream gis = new GZIPInputStream(is);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int bytes_read;
        while ((bytes_read = gis.read(buffer)) != -1) {
            bos.write(buffer, 0, bytes_read);
        }
        gis.close();
        requestInfo.setBody(bos.toString());
        requestInfo.setUrl(request.getRequestURI());
        requestInfo.setMethod(request.getMethod());
        requestInfo.setTimeStamp(System.currentTimeMillis());
        Enumeration<String> headerNames = request.getHeaderNames();
        List<String> headers = new ArrayList<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            Enumeration<String> requestHeaders = request.getHeaders(headerName);
            while (requestHeaders.hasMoreElements()) {
                String headerValue = requestHeaders.nextElement();
                headers.add(headerName + ":" + headerValue);
            }
        }
        requestInfo.setHeaders(headers);
        return requestInfo;
    }

    public List<RequestInfo> getCapturedRequests() {
        return capturedRequests;
    }

    public void flushCapturedRequests() {
        capturedRequests.clear();
    }
}

