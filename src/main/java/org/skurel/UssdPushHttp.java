package org.skurel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class UssdPushHttp {
    private static final Logger log = LoggerFactory.getLogger(UssdPushHttp.class);

    private final String ip;
    private final String port;
    private final String userId;
    private final String password;
    private final String serviceCode;
    private final HttpClient httpClient;

    public UssdPushHttp(String ip, String port, String userId, String password,
                        String serviceCode) {
        this.ip = ip;
        this.port = port;
        this.userId = userId;
        this.password = password;
        this.serviceCode = serviceCode;
        SSLParameters sslParams = new SSLParameters();
        sslParams.setEndpointIdentificationAlgorithm(null);

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .sslContext(trustAllCerts())
                .sslParameters(sslParams)
                .build();
    }

    public void push(String msisdn, String sessionId, String input) {
        String pushUrl = "https://" + ip + ":" + port +
                "/ussdhttpquery/qs?PUSH_TYPE=3" +
                "&SERVICE_CODE=" + URLEncoder.encode(serviceCode, StandardCharsets.UTF_8) +
                "&USER_ID=" + URLEncoder.encode(userId, StandardCharsets.UTF_8) +
                "&PASSWORD=" + URLEncoder.encode(password, StandardCharsets.UTF_8) +
                "&MSISDN=" + URLEncoder.encode(msisdn, StandardCharsets.UTF_8) +
                "&PUSH_TEXT=" + URLEncoder.encode(input, StandardCharsets.UTF_8) +
                "&OPT_PARAM=" +
                "&SESSION_ID=" + URLEncoder.encode(sessionId, StandardCharsets.UTF_8);

        log.info("event=airtel_push_http_start url={}", pushUrl);

        httpGet(pushUrl).thenAccept(v -> {
            log.info("event=airtel_push_http_complete url={}", pushUrl);
        }).exceptionally(e -> {
            log.error("event=airtel_push_http_failed url={} error={}", pushUrl, e.getMessage(), e);
            return null;
        });
    }

    private static SSLContext trustAllCerts() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{
                    new X509ExtendedTrustManager() {
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}
                        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}
                        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}
                        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}
                        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    }
            }, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create trust-all SSLContext", e);
        }
    }

    private CompletableFuture<Void> httpGet(String url) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    int status = response.statusCode();
                    if (status < 200 || status >= 300) {
                        log.warn("event=airtel_push_http_non_200 status={} url={}", status, url);
                    }
                });
    }
}
