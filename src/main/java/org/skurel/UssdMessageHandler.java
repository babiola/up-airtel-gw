package org.skurel;

import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.*;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.util.AbsoluteTimeFormatter;
import org.jsmpp.util.TimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class UssdMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(UssdMessageHandler.class);
    private static final TimeFormatter TIME_FORMATTER = new AbsoluteTimeFormatter();

    private static final int BUFFER_MSISDN = 100;
    private static final int BUFFER_INPUT = 109;
    private static final int BUFFER_SESSIONID = 110;
    private static final int BUFFER_APP_RESPONSE = 121;
    private static final int BUFFER_NEWREQUEST = 123;
    private static final int BUFFER_FREEFLOW = 117;

    private final SmppConnectionPool connectionPool;
    private final String processUrl;
    private final String serviceCode;
    private final String serviceType;
    private final ExecutorService workerPool;
    private final HttpClient httpClient;

    private final AtomicLong deliverSmCount = new AtomicLong();
    private final AtomicLong submitSmCount = new AtomicLong();
    private final AtomicLong httpErrorCount = new AtomicLong();
    private final AtomicLong sendErrorCount = new AtomicLong();

    public UssdMessageHandler(SmppConnectionPool connectionPool, String processUrl,
                              String serviceCode, String serviceType, int workerThreads) {
        this.connectionPool = connectionPool;
        this.processUrl = processUrl;
        this.serviceCode = serviceCode;
        this.serviceType = serviceType;
        this.workerPool = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "ussd-worker");
            t.setDaemon(true);
            return t;
        });
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .executor(Executors.newFixedThreadPool(workerThreads))
                .build();
    }

    public void processDeliverSm(SMPPSession session, DeliverSm deliverSm) {
        if (MessageType.SMSC_DEL_RECEIPT.containedIn(deliverSm.getEsmClass())) {
            return;
        }
        deliverSmCount.incrementAndGet();
        workerPool.execute(() -> doProcess(session, deliverSm));
    }

    private void doProcess(SMPPSession session, DeliverSm deliverSm) {
        try {
            byte[] shortMessage = deliverSm.getShortMessage();
            if (shortMessage == null || shortMessage.length == 0) return;
            String payload = new String(shortMessage);

            String sessionid = decodeBase64(bufferValue(BUFFER_SESSIONID, payload));
            String isNewRequest = decodeBase64(bufferValue(BUFFER_NEWREQUEST, payload));
            String input = decodeBase64(bufferValue(BUFFER_INPUT, payload));
            String msisdn = decodeBase64(bufferValue(BUFFER_MSISDN, payload));

            input = isNewRequest.equals("1") ? "*" + input + "#" : input;
            log.info("MSISDN: {} INPUT: {} NEWREQUEST: {} SESSION: {}", msisdn, input, isNewRequest, sessionid);

            String url = processUrl
                    + "?msisdn=" + msisdn
                    + "&sessionid=" + sessionid
                    + "&input=" + URLEncoder.encode(input, StandardCharsets.UTF_8)
                    + "&network=Airtel";

            callHttpAsync(url, menu -> {
                if (menu == null || menu.isEmpty()) return;
                sendSubmitSm(session, msisdn, menu, sessionid);
            });

        } catch (Exception e) {
            log.error("Error processing deliver_sm: {}", e.getMessage());
        }
    }

    public void sendPushInit(String msisdn, String msg) {
        SMPPSession session = connectionPool.nextHealthySession();
        if (session == null) {
            log.warn("No healthy session for push USSD to {}", msisdn);
            sendErrorCount.incrementAndGet();
            return;
        }

        final RegisteredDelivery registeredDelivery = new RegisteredDelivery();
        registeredDelivery.setSMSCDeliveryReceipt(SMSCDeliveryReceipt.SUCCESS_FAILURE);

        try {
            OptionalParameter.Byte optionParam = new OptionalParameter.Byte(
                    OptionalParameter.Tag.USSD_SERVICE_OP, (byte) 1);

            String response = prepairdResponse(msisdn, "", msg, "FC");
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            if (payload.length > 254) {
                log.warn("Payload too long: {} bytes. Truncating.", payload.length);
                payload = Arrays.copyOf(payload, 254);
            }

            session.submitShortMessage(
                    serviceType,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, serviceCode,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msisdn,
                    new ESMClass(), (byte) 0, (byte) 1,
                    TIME_FORMATTER.format(new Date()), null,
                    registeredDelivery, (byte) 0,
                    DataCodings.ZERO, (byte) 0,
                    payload, optionParam);

            submitSmCount.incrementAndGet();
            log.info("Push USSD init sent to {} via round-robin", msisdn);

        } catch (PDUException | ResponseTimeoutException | InvalidResponseException
                 | NegativeResponseException | IOException e) {
            log.error("Failed to send push USSD to {}: {}", msisdn, e.getMessage());
            sendErrorCount.incrementAndGet();
        }
    }

    private void sendSubmitSm(SMPPSession preferredSession, String destination, String msg, String sessionid) {
        String freeflow = "FC";
        final RegisteredDelivery registeredDelivery = new RegisteredDelivery();
        registeredDelivery.setSMSCDeliveryReceipt(SMSCDeliveryReceipt.SUCCESS_FAILURE);

        try {
            OptionalParameter.Byte optionParam;
            String endSession = msg.length() >= 3 ? msg.substring(0, 3).toUpperCase() : "";
            if ("END".equals(endSession)) {
                optionParam = new OptionalParameter.Byte(OptionalParameter.Tag.USSD_SERVICE_OP, (byte) 17);
                msg = msg.replaceFirst("(?i)END", "").trim();
                freeflow = "FB";
            } else {
                if ("CON".equals(endSession)) {
                    msg = msg.replaceFirst("(?i)CON", "").trim();
                }
                optionParam = new OptionalParameter.Byte(OptionalParameter.Tag.USSD_SERVICE_OP, (byte) 2);
            }

            String response = prepairdResponse(destination, sessionid, msg, freeflow);
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            if (payload.length > 254) {
                log.warn("Payload too long: {} bytes. Truncating.", payload.length);
                payload = Arrays.copyOf(payload, 254);
            }

            SMPPSession sendSession = resolveSendSession(preferredSession);
            if (sendSession == null) {
                log.warn("No healthy session to send submit_sm to {}", destination);
                sendErrorCount.incrementAndGet();
                return;
            }

            sendSession.submitShortMessage(
                    serviceType,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, serviceCode,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, destination,
                    new ESMClass(), (byte) 0, (byte) 1,
                    TIME_FORMATTER.format(new Date()), null,
                    registeredDelivery, (byte) 0,
                    DataCodings.ZERO, (byte) 0,
                    payload, optionParam);

            submitSmCount.incrementAndGet();
            log.info("submit_sm sent | freeflow={}", freeflow);

        } catch (PDUException | ResponseTimeoutException | InvalidResponseException
                 | NegativeResponseException | IOException e) {
            log.error("Failed to send submit_sm to {}: {}", destination, e.getMessage());
            sendErrorCount.incrementAndGet();
        }
    }

    private SMPPSession resolveSendSession(SMPPSession preferred) {
        if (preferred != null && preferred.getSessionState().isBound()) {
            return preferred;
        }
        return connectionPool.nextHealthySession();
    }

    private void callHttpAsync(String url, Consumer<String> callback) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAcceptAsync(response -> {
                        int status = response.statusCode();
                        if (status >= 200 && status < 300) {
                            callback.accept(response.body().trim());
                        } else {
                            log.warn("HTTP {} from USSD app", status);
                            httpErrorCount.incrementAndGet();
                        }
                    }, workerPool)
                    .exceptionally(ex -> {
                        log.error("HTTP call failed: {}", ex.getMessage());
                        httpErrorCount.incrementAndGet();
                        return null;
                    });
        } catch (Exception e) {
            log.error("HTTP call failed: {}", e.getMessage());
            httpErrorCount.incrementAndGet();
        }
    }

    String prepairdResponse(String msisdn, String sessionid, String msg, String freeflow) {
        return "1|"
                + BUFFER_MSISDN + ":" + encodeBase64(msisdn) + "|"
                + BUFFER_SESSIONID + ":" + encodeBase64(sessionid) + "|"
                + BUFFER_APP_RESPONSE + ":" + encodeBase64(msg) + "|"
                + BUFFER_FREEFLOW + ":" + encodeBase64(freeflow);
    }

    String bufferValue(int bufferId, String encryptedText) {
        String value = "0";
        String[] splitString = encryptedText.split("\\|");
        for (String segment : splitString) {
            if (bufferId == 0) {
                value = splitString[0];
                break;
            }
            String[] keyval = segment.split(":");
            if (keyval[0].equals(Integer.toString(bufferId))) {
                value = keyval[1];
                break;
            }
        }
        return value;
    }

    String encodeBase64(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
    }

    String decodeBase64(String encodedStr) {
        try {
            return new String(Base64.getDecoder().decode(encodedStr), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "";
        }
    }

    public void shutdown() {
        workerPool.shutdown();
        try { workerPool.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public AtomicLong getDeliverSmCount() { return deliverSmCount; }
    public AtomicLong getSubmitSmCount() { return submitSmCount; }
    public AtomicLong getHttpErrorCount() { return httpErrorCount; }
    public AtomicLong getSendErrorCount() { return sendErrorCount; }
}
