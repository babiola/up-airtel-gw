package org.skurel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AirtelUssdGw {
    private static final Logger log = LoggerFactory.getLogger(AirtelUssdGw.class);

    private static final int DEFAULT_PORT = 2775;
    private static final int DEFAULT_WORKER_THREADS = 128;
    private static final int DEFAULT_PUSH_PORT = 8020;

    public static void main(String[] args) {
        Properties config = loadConfig();

        String hostsProp = config.getProperty("smpp.hosts");
        String portProp = config.getProperty("smpp.port", String.valueOf(DEFAULT_PORT));
        String systemId = config.getProperty("smpp.systemId");
        String password = config.getProperty("smpp.password");
        String processUrl = config.getProperty("ussd.processUrl");
        String serviceCode = config.getProperty("ussd.serviceCode", "");
        String serviceType = config.getProperty("ussd.serviceType", "USSD");
        boolean testMode = "true".equalsIgnoreCase(config.getProperty("ussd.testMode", "false"));
        int pushPort = Integer.parseInt(config.getProperty("ussd.push.port", String.valueOf(DEFAULT_PUSH_PORT)));
        String pushMethod = config.getProperty("ussd.push.method", "smpp");

        if (isBlank(hostsProp) || isBlank(systemId) || isBlank(password) || isBlank(processUrl)) {
            log.error("Missing required config: smpp.hosts, smpp.systemId, smpp.password, ussd.processUrl");
            System.exit(1);
        }

        List<String> hosts = Arrays.asList(hostsProp.trim().split("\\s*,\\s*"));
        int port = Integer.parseInt(portProp.trim());
        int workerThreads = Integer.parseInt(config.getProperty("worker.threads", String.valueOf(DEFAULT_WORKER_THREADS)));

        log.info("=== Airtel USSD Gateway ===");
        log.info("SMPP hosts: {}", hosts);
        log.info("SMPP port: {}", port);
        log.info("SMPP systemId: {}", systemId);
        log.info("Service code: {}", serviceCode);
        log.info("Service type: {}", serviceType);
        if (testMode) log.warn("TEST MODE enabled - will not call HTTP app");
        log.info("Push HTTP port: {}", pushPort);
        log.info("Worker threads: {}", workerThreads);

        try {
            SmppConnectionPool connectionPool = new SmppConnectionPool(hosts, port, systemId, password);

            UssdMessageHandler messageHandler = new UssdMessageHandler(connectionPool, processUrl, serviceCode, serviceType, workerThreads, testMode);

            connectionPool.start(messageHandler);

            UssdPushHttp httpPush = null;
            if ("http".equalsIgnoreCase(pushMethod)) {
                String pushIp = config.getProperty("ussd.push.http.ip");
                String pushHttpPort = config.getProperty("ussd.push.http.port");
                String pushUserId = config.getProperty("ussd.push.http.userid");
                String pushPassword = config.getProperty("ussd.push.http.password");
                String pushServiceCode = config.getProperty("ussd.push.http.serviceCode");
                String pushTypeValue = config.getProperty("ussd.push.http.typeValue", "1");
                httpPush = new UssdPushHttp(pushIp, pushHttpPort, pushUserId, pushPassword,
                        pushServiceCode, pushTypeValue);
                log.info("Using HTTP push method");
            }

            UssdPushServer pushServer = new UssdPushServer(pushPort, connectionPool, messageHandler, httpPush);
            pushServer.start();

            Thread monitorThread = startTrafficMonitor(messageHandler, connectionPool);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown initiated...");
                monitorThread.interrupt();
                pushServer.shutdown();
                messageHandler.shutdown();
                connectionPool.shutdown();
                log.info("Shutdown complete");
            }, "shutdown-hook"));

            log.info("Gateway started. {} active connections.", connectionPool.activeConnectionCount());
            Thread.currentThread().join();

        } catch (Exception e) {
            log.error("Failed to start gateway", e);
            System.exit(1);
        }
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        boolean loaded = false;

        File externalFile = new File("config.properties");
        if (externalFile.exists()) {
            try (FileInputStream fis = new FileInputStream(externalFile)) {
                props.load(fis);
                //log.info("Loaded config from {}", externalFile.getAbsolutePath());
                loaded = true;
            } catch (IOException e) {
                log.warn("Failed to load external config: {}", e.getMessage());
            }
        }

        if (!loaded) {
            try (InputStream is = AirtelUssdGw.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (is != null) {
                    props.load(is);
                    //log.info("Loaded config from classpath");
                } else {
                    log.warn("No config.properties found on classpath");
                }
            } catch (IOException e) {
                log.warn("Failed to load classpath config: {}", e.getMessage());
            }
        }

        for (String key : props.stringPropertyNames()) {
            String sysVal = System.getProperty(key);
            if (sysVal != null) {
                props.setProperty(key, sysVal);
            }
        }

        return props;
    }

    private static Thread startTrafficMonitor(UssdMessageHandler handler, SmppConnectionPool pool) {
        Thread monitor = new Thread(() -> {
            long lastDeliver = 0;
            long lastSubmit = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);

                    long totalDeliver = handler.getDeliverSmCount().get();
                    long totalSubmit = handler.getSubmitSmCount().get();
                    long httpErrors = handler.getHttpErrorCount().get();
                    long sendErrors = handler.getSendErrorCount().get();
                    long negResp = handler.getNegRespCount().get();
                    long timeouts = handler.getTimeoutCount().get();
                    long ioErrs = handler.getIoErrCount().get();

                    long tpsDeliver = (totalDeliver - lastDeliver) / 5;
                    long tpsSubmit = (totalSubmit - lastSubmit) / 5;
                    lastDeliver = totalDeliver;
                    lastSubmit = totalSubmit;

                    int activeConns = pool.activeConnectionCount();

                    log.info("Traffic | deliver={}/s submit={}/s http_err={} send_err={} [negResp={} timeout={} ioErr={}] conns={}/{} | Total deliver={} submit={}",
                            tpsDeliver, tpsSubmit, httpErrors, sendErrors, negResp, timeouts, ioErrs, activeConns, 4,
                            totalDeliver, totalSubmit);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "traffic-monitor");
        monitor.setDaemon(true);
        monitor.start();
        return monitor;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
