package org.skurel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class UssdPushHttp {
    private static final Logger log = LoggerFactory.getLogger(UssdPushHttp.class);

    private static final String URL_TYPE_1_PUSH = "http://<IP>:<PUSH_IA_PORT>/ussdhttpquery/qs?TYPE=<TYPE_VALUE>&PUSH_TYPE=1&SERVICE_CODE=<SERVICE_CODE_VALUE>&MSISDN=<MSISDN_VALUE>&PUSH_TEXT=<PUSH_TEXT_VALUE>&USER_ID=<USER_ID_VALUE>&PASSWORD=<PASSWORD_VALUE>";

    private static final String URL_TYPE_2_QUERY = "http://<IP>:<PUSH_IA_PORT>/ussdhttpquery/qs?PUSH_TYPE=2&SERVICE_CODE=<SERVICE_CODE_VALUE>&MSISDN=<MSISDN_VALUE>&USER_ID=<USER_ID>&PASSWORD=<PASSWORD>";

    private static final String URL_TYPE_3_NOTIFY = "http://<IP>:<PUSH_IA_PORT>/ussdhttpquery/qs?PUSH_TYPE=3&SERVICE_CODE=<SERVICE_CODE_VALUE>&MSISDN=<MSISDN_VALUE>&PUSH_TEXT=<PUSH_TEXT>&USER_ID=<USER_ID>&PASSWORD=<PASSWORD>";

    private final String ip;
    private final String port;
    private final String userId;
    private final String password;
    private final String serviceCode;
    private final String typeValue;
    private final HttpClient httpClient;

    public UssdPushHttp(String ip, String port, String userId, String password,
                        String serviceCode, String typeValue) {
        this.ip = ip;
        this.port = port;
        this.userId = userId;
        this.password = password;
        this.serviceCode = serviceCode;
        this.typeValue = typeValue;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public boolean push(String type, String msisdn, String pushText) {
        String template;
        switch (type) {
            case "1":
                template = URL_TYPE_1_PUSH
                    .replace("<TYPE_VALUE>", typeValue);
                break;
            case "2":
                template = URL_TYPE_2_QUERY;
                break;
            case "3":
                template = URL_TYPE_3_NOTIFY;
                break;
            default:
                log.warn("Unknown push type: {}, defaulting to 1", type);
                template = URL_TYPE_1_PUSH
                    .replace("<TYPE_VALUE>", typeValue);
                break;
        }

        String url = template
                .replace("<IP>", ip)
                .replace("<PUSH_IA_PORT>", port)
                .replace("<SERVICE_CODE_VALUE>", serviceCode)
                .replace("<MSISDN_VALUE>", msisdn)
                .replace("<USER_ID_VALUE>", userId)
                .replace("<USER_ID>", userId)
                .replace("<PASSWORD_VALUE>", password)
                .replace("<PASSWORD>", password);

        if (pushText != null) {
            String encoded = URLEncoder.encode(pushText, StandardCharsets.UTF_8);
            url = url.replace("<PUSH_TEXT_VALUE>", encoded)
                     .replace("<PUSH_TEXT>", encoded);
        } else {
            url = url.replace("<PUSH_TEXT_VALUE>", "")
                     .replace("<PUSH_TEXT>", "");
        }

        try {
            log.info("HTTP push | type={} msisdn={} url={}", type, msisdn, url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("HTTP push response | type={} msisdn={} status={} body={}",
                    type, msisdn, response.statusCode(), response.body());
            return response.statusCode() >= 200 && response.statusCode() < 300;

        } catch (Exception e) {
            log.error("HTTP push failed | type={} msisdn={}", type, msisdn, e);
            return false;
        }
    }
}
