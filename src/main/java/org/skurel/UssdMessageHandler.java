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
    private static final int BUFFER_STATUS_CODE = 116;
    private static final int BUFFER_FREEFLOW = 117;
    private static final int BUFFER_APP_RESPONSE = 121;
    private static final int BUFFER_NEWREQUEST = 123;

    private final SmppConnectionPool connectionPool;
    private final String processUrl;
    private final String serviceCode;
    private final String serviceType;
    private final boolean testMode;
    private final ExecutorService workerPool;
    private final HttpClient httpClient;

    private final AtomicLong deliverSmCount = new AtomicLong();
    private final AtomicLong submitSmCount = new AtomicLong();
    private final AtomicLong httpErrorCount = new AtomicLong();
    private final AtomicLong sendErrorCount = new AtomicLong();
    private final AtomicLong negRespCount = new AtomicLong();
    private final AtomicLong timeoutCount = new AtomicLong();
    private final AtomicLong ioErrCount = new AtomicLong();

    public UssdMessageHandler(SmppConnectionPool connectionPool, String processUrl,
                              String serviceCode, String serviceType, int workerThreads, boolean testMode) {
        this.connectionPool = connectionPool;
        this.processUrl = processUrl;
        this.serviceCode = serviceCode;
        this.serviceType = serviceType;
        this.testMode = testMode;
        this.workerPool = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "ussd-worker");
            t.setDaemon(true);
            return t;
        });
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .executor(Executors.newFixedThreadPool(workerThreads, r -> {
                    Thread t = new Thread(r, "http-worker");
                    t.setDaemon(true);
                    return t;
                }))
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
            //System.out.println("RAW_INPUT: {}" +deliverSm.getShortMessage());
            byte[] shortMessage = deliverSm.getShortMessage();
            if (shortMessage == null || shortMessage.length == 0) return;
            String payload = new String(shortMessage);
            log.info("RAW_INPUT: {}", payload);

            // MSISDN from buffer ID 100 (Airtel SMSC uses this, not SMPP address)
            String msisdnBuf = decodeBase64(bufferValue(BUFFER_MSISDN, payload));
            String msisdn = (msisdnBuf != null && !msisdnBuf.isEmpty()) ? msisdnBuf : deliverSm.getDestAddress();

            // Read USSD_SERVICE_OP TLV from deliver_sm
            byte ussdServiceOp = -1;
            OptionalParameter.Byte ussdOpParam = (OptionalParameter.Byte) deliverSm.getOptionalParameter(
                    OptionalParameter.Tag.USSD_SERVICE_OP);
            if (ussdOpParam != null) {
                ussdServiceOp = ussdOpParam.getValue();
            }

            // Check for cleanup request (Message Type = 2 per spec)
            String msgType = payload.split("\\|", 2)[0].trim();
            if ("2".equals(msgType)) {
                String statusCode = decodeBase64(bufferValue(BUFFER_STATUS_CODE, payload));
                log.info("[CLEANUP] Session cleanup for MSISDN={} status={} ussdServiceOp=0x{}",
                        msisdn, statusCode, Integer.toHexString(ussdServiceOp));
                return; // No submit_sm response expected for cleanup
            }

            String sessionid = decodeBase64(bufferValue(BUFFER_SESSIONID, payload));
            String isNewRequest = decodeBase64(bufferValue(BUFFER_NEWREQUEST, payload));
            String input = decodeBase64(bufferValue(BUFFER_INPUT, payload));

            log.info("DELIVER_SM_IN:: MSISDN: {} | INPUT: {} | SESSION: {} | NEWREQUEST: {} | SERVICE_CODE: {} | TIME: {} | NETWORK: {}", msisdn, input, sessionid, isNewRequest, serviceCode, System.currentTimeMillis(), "airtel");

            input = isNewRequest.equals("1") ? "*" + input + "#" : input;

            long startNanos = System.nanoTime();
            //log.info("[TRACE] >> MSISDN={} session={} input={} | processing", msisdn, sessionid, input);

            if (testMode) {
                sendSubmitSm(session, msisdn, "Welcome to ABC test\n1.Balance\n2.Open account", sessionid, startNanos);
                return;
            }

            String url = processUrl
                    + "?msisdn=" + msisdn
                    + "&sessionid=" + sessionid
                    + "&input=" + URLEncoder.encode(input, StandardCharsets.UTF_8)
                    + "&network=airtel&ussdPort=8210";
            log.info("[HTTP] >> {} to {}", url, msisdn);

            callHttpAsync(url, session, msisdn, sessionid, startNanos, menu -> {
                if (menu == null || menu.isEmpty()) return;
                sendSubmitSm(session, msisdn, menu, sessionid, startNanos);
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

        long smppStart = 0;
        try {
            OptionalParameter.Byte optionParam = new OptionalParameter.Byte(
                    OptionalParameter.Tag.USSD_SERVICE_OP, (byte) 1);

            String response = prepairdResponse(msisdn, "", msg, "FC");
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            if (payload.length > 254) {
                log.warn("Payload too long: {} bytes. Truncating.", payload.length);
                payload = Arrays.copyOf(payload, 254);
            }

            smppStart = System.nanoTime();
            session.submitShortMessage(
                    serviceType,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, serviceCode,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, msisdn,
                    new ESMClass(), (byte) 0, (byte) 1,
                    TIME_FORMATTER.format(new Date()), null,
                    registeredDelivery, (byte) 0,
                    DataCodings.ZERO, (byte) 0,
                    payload, optionParam);
            long smppElapsed = (System.nanoTime() - smppStart) / 1_000_000;

            submitSmCount.incrementAndGet();
            log.info("SUBMIT_SM_OUT:: PUSH MSISDN={} | SMPP-RESP={}ms", msisdn, smppElapsed);

        } catch (PDUException | ResponseTimeoutException | InvalidResponseException
                 | NegativeResponseException | IOException e) {
            log.error("Failed to send push USSD to {}: {} [{}]", msisdn, e.getClass().getSimpleName(), e.getMessage(), e);
            if (e instanceof NegativeResponseException) {
                int status = ((NegativeResponseException) e).getCommandStatus();
                log.warn("SMSC rejected push USSD to {}: command_status=0x{}", msisdn, Integer.toHexString(status));
                negRespCount.incrementAndGet();
            } else if (e instanceof ResponseTimeoutException || e instanceof InvalidResponseException) {
                timeoutCount.incrementAndGet();
            } else {
                ioErrCount.incrementAndGet();
            }
            sendErrorCount.incrementAndGet();
        }
    }

    private void sendSubmitSm(SMPPSession preferredSession, String destination, String msg, String sessionid, long startNanos) {
        String freeflow = "FC";
        final RegisteredDelivery registeredDelivery = new RegisteredDelivery();
        registeredDelivery.setSMSCDeliveryReceipt(SMSCDeliveryReceipt.SUCCESS_FAILURE);

        long smppStart = 0;
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
            //if (msg.length() > 160) msg = msg.substring(0, 160);

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

            smppStart = System.nanoTime();
            sendSession.submitShortMessage(
                    serviceType,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, serviceCode,
                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, destination,
                    new ESMClass(), (byte) 0, (byte) 1,
                    TIME_FORMATTER.format(new Date()), null,
                    registeredDelivery, (byte) 0,
                    DataCodings.ZERO, (byte) 0,
                    payload, optionParam);
            long smppElapsed = (System.nanoTime() - smppStart) / 1_000_000;

            submitSmCount.incrementAndGet();
            long httpElapsed = (smppStart - startNanos) / 1_000_000;
            log.info("SUBMIT_SM_OUT:: MSISDN={} | SP-RES={}ms | SMPP-RESP={}ms | SESSION={}",
                    destination, httpElapsed, smppElapsed, sessionid);

            long totalElapsed = (System.nanoTime() - startNanos) / 1_000_000;
            long endTime = System.currentTimeMillis();
            log.info("USSD_SESSION:: SESSION_ID={} | PHONE_NUMBER={} | START_TIME={} | TOTAL_TIME={}ms | END_TIME={} | SHORT_CODE={} | NETWORK=AIRTEL",
                    sessionid, destination, (endTime - totalElapsed), totalElapsed, endTime, serviceCode);

        } catch (PDUException | ResponseTimeoutException | InvalidResponseException
                 | NegativeResponseException | IOException e) {
            long smppElapsed = (System.nanoTime() - smppStart) / 1_000_000;
            long httpElapsed = (smppStart - startNanos) / 1_000_000;
            log.error("SUBMIT_SM_OUT:: MSISDN={} | SP-RES={}ms | SMPP-RESP={}ms FAILED | SESSION={} | {}",
                    destination, httpElapsed, smppElapsed, sessionid, e.getClass().getSimpleName(), e);
            if (e instanceof NegativeResponseException) {
                int status = ((NegativeResponseException) e).getCommandStatus();
                log.warn("SMSC rejected submit_sm to {}: command_status=0x{}", destination, Integer.toHexString(status));
                negRespCount.incrementAndGet();
            } else if (e instanceof ResponseTimeoutException || e instanceof InvalidResponseException) {
                timeoutCount.incrementAndGet();
            } else {
                ioErrCount.incrementAndGet();
            }
            sendErrorCount.incrementAndGet();
        }
    }

    private SMPPSession resolveSendSession(SMPPSession preferred) {
        if (preferred != null && preferred.getSessionState().isBound()) {
            return preferred;
        }
        return connectionPool.nextHealthySession();
    }

    private void callHttpAsync(String url, SMPPSession session, String msisdn, String sessionid, long startNanos, Consumer<String> callback) {
        try {
            // Fix 1: Hard timeout at the HTTP Request layer to safely close sockets
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAcceptAsync(response -> {
                        long elapsed = (System.nanoTime() - startNanos) / 1_000_000;
                        int status = response.statusCode();
                        if (status >= 200 && status < 300) {
                            String body = response.body().trim();
                            log.info("[TRACE] << MSISDN={} session={} | HTTP 200 in {}ms | body={}", msisdn, sessionid, elapsed, body);
                            callback.accept(body);
                        } else {
                            log.warn("HTTP {} from USSD app for MSISDN={} in {}ms", status, msisdn, elapsed);
                            httpErrorCount.incrementAndGet();
                            sendSubmitSm(session, msisdn, "Please try again, the request timeout", sessionid, startNanos);
                        }
                    }, workerPool)
                    // Fix 2: Use exceptionallyAsync to protect your system threads
                    .exceptionallyAsync(ex -> {
                        long elapsed = (System.nanoTime() - startNanos) / 1_000_000;
                        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;

                        log.error("HTTP call failed for MSISDN={} in {}ms: {}", msisdn, elapsed, cause.getMessage());
                        httpErrorCount.incrementAndGet();

                        // Fix 3: Handle both standard timeouts and Java HTTPClient internal timeouts
                        String errMsg = (cause instanceof java.net.http.HttpTimeoutException || cause instanceof java.util.concurrent.TimeoutException)
                                ? "Please try again, the request timeout"
                                : "Please try again, the response took longer";

                        sendSubmitSm(session, msisdn, errMsg, sessionid, startNanos);
                        return null;
                    }, workerPool); // Pass your pool here

        } catch (Exception e) {
            long elapsed = (System.nanoTime() - startNanos) / 1_000_000;
            log.error("HTTP call failed for MSISDN={} in {}ms: {}", msisdn, elapsed, e.getMessage());
            httpErrorCount.incrementAndGet();
            sendSubmitSm(session, msisdn, "Please try again, the request timeout", sessionid, startNanos);
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
    public AtomicLong getNegRespCount() { return negRespCount; }
    public AtomicLong getTimeoutCount() { return timeoutCount; }
    public AtomicLong getIoErrCount() { return ioErrCount; }
}
