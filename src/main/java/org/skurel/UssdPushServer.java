package org.skurel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class UssdPushServer {
    private static final Logger log = LoggerFactory.getLogger(UssdPushServer.class);

    private final int port;
    private final SmppConnectionPool connectionPool;
    private final UssdMessageHandler handler;
    private final UssdPushHttp httpPush;
    private HttpServer server;

    public UssdPushServer(int port, SmppConnectionPool connectionPool, UssdMessageHandler handler) {
        this(port, connectionPool, handler, null);
    }

    public UssdPushServer(int port, SmppConnectionPool connectionPool, UssdMessageHandler handler, UssdPushHttp httpPush) {
        this.port = port;
        this.connectionPool = connectionPool;
        this.handler = handler;
        this.httpPush = httpPush;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "push-http");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/", this::handleRequest);
        server.start();
        log.info("Push HTTP server started on port {}", port);
    }

    public void shutdown() {
        if (server != null) {
            server.stop(2);
            log.info("Push HTTP server stopped");
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        try {
            String query = exchange.getRequestURI().getRawQuery();
            if (query == null) {
                respond(exchange, 400, "Missing query parameters");
                return;
            }

            Map<String, String> params = parseQuery(query);

            String msisdn = params.get("msisdn");
            String input = params.get("input");
            String type = params.get("type");
            if (type == null) type = "1";

            if (msisdn == null || input == null) {
                respond(exchange, 400, "Missing msisdn or input");
                return;
            }

            input = URLDecoder.decode(input, StandardCharsets.UTF_8);

            log.info("Push USSD | type={} msisdn={} input={}", type, msisdn, input);

            if (httpPush != null) {
                httpPush.push(type, msisdn, input);
            } else {
                handler.sendPushInit(msisdn, input);
            }
            respond(exchange, 200, "OK");

        } catch (Exception e) {
            log.error("Push request failed", e);
            respond(exchange, 500, "Internal error");
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2) {
                map.put(pair[0], pair[1]);
            }
        }
        return map;
    }

    private void respond(HttpExchange exchange, int code, String body) throws IOException {
        exchange.sendResponseHeaders(code, body.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }
}