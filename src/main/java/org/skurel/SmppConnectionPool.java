package org.skurel;

import org.jsmpp.bean.*;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SmppConnectionPool {
    private static final Logger log = LoggerFactory.getLogger(SmppConnectionPool.class);
    private static final long TRANSACTION_TIMER = 30000L;
    private static final int ENQUIRELINK_INTERVAL = 15000;
    private static final int PDU_PROCESSOR_DEGREE = 10;

    private final List<String> hosts;
    private final int port;
    private final String systemId;
    private final String password;

    private final List<SmppConnection> connections = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger roundRobinIdx = new AtomicInteger(0);

    public SmppConnectionPool(List<String> hosts, int port, String systemId, String password) {
        this.hosts = hosts;
        this.port = port;
        this.systemId = systemId;
        this.password = password;
    }

    public void start(UssdMessageHandler handler) {
        int idx = 1;
        for (String host : hosts) {
            SmppConnection conn = new SmppConnection(host, idx++, handler);
            conn.start();
            connections.add(conn);
        }
        log.info("Started {} SMPP connections", connections.size());
    }

    public void shutdown() {
        running.set(false);
        for (SmppConnection conn : connections) {
            conn.stop();
        }
        connections.clear();
        log.info("All SMPP connections shut down");
    }

    public SMPPSession nextHealthySession() {
        int size = connections.size();
        if (size == 0) return null;

        for (int i = 0; i < size; i++) {
            int idx = Math.abs(roundRobinIdx.getAndIncrement() % size);
            SmppConnection conn = connections.get(idx);
            SMPPSession session = conn.getActiveSession();
            if (session != null && session.getSessionState().isBound()) {
                return session;
            }
        }
        return null;
    }

    public SMPPSession getSession(int index) {
        for (SmppConnection conn : connections) {
            if (conn.connIndex == index) {
                SMPPSession session = conn.getActiveSession();
                if (session != null && session.getSessionState().isBound()) {
                    return session;
                }
                break;
            }
        }
        return nextHealthySession();
    }

    public int activeConnectionCount() {
        int count = 0;
        for (SmppConnection conn : connections) {
            SMPPSession s = conn.getActiveSession();
            if (s != null && s.getSessionState().isBound()) count++;
        }
        return count;
    }

    private class SmppConnection {
        private final String host;
        private final int connIndex;
        private final UssdMessageHandler handler;
        private final AtomicReference<SMPPSession> activeSessionRef = new AtomicReference<>();
        private Thread reconnectThread;

        SmppConnection(String host, int connIndex, UssdMessageHandler handler) {
            this.host = host;
            this.connIndex = connIndex;
            this.handler = handler;
        }

        void start() {
            reconnectThread = new Thread(this::reconnectLoop, "smpp-conn-" + host);
            reconnectThread.setDaemon(true);
            reconnectThread.start();
        }

        void stop() {
            reconnectThread.interrupt();
            SMPPSession session = activeSessionRef.getAndSet(null);
            if (session != null) {
                try { session.close(); } catch (Exception ignored) {}
            }
        }

        SMPPSession getActiveSession() {
            return activeSessionRef.get();
        }

        private void reconnectLoop() {
            int backoffSeconds = 1;
            while (running.get()) {
                SMPPSession session = null;
                try {
                    session = new SMPPSession();
                    session.setEnquireLinkTimer(ENQUIRELINK_INTERVAL);
                    session.setTransactionTimer(TRANSACTION_TIMER);
                    session.setPduProcessorDegree(PDU_PROCESSOR_DEGREE);

                    session.setMessageReceiverListener(new MessageReceiverListener() {
                        public void onAcceptDeliverSm(DeliverSm deliverSm) {
                            SMPPSession current = activeSessionRef.get();
                            handler.processDeliverSm(current, deliverSm);
                        }

                        public DataSmResult onAcceptDataSm(DataSm dataSm, Session source) {
                            return null;
                        }

                        public void onAcceptAlertNotification(AlertNotification alertNotification) {
                        }
                    });

                    BindParameter bindParam = new BindParameter(BindType.BIND_TRX, systemId, password, "CP",
                            TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null, InterfaceVersion.IF_50);

                    CountDownLatch closeLatch = new CountDownLatch(1);
                    session.addSessionStateListener((newState, oldState, source) -> {
                        if (newState == SessionState.CLOSED || newState == SessionState.UNBOUND) {
                            log.warn("Session {}:{} dropped ({} -> {}), will reconnect", host, port, oldState, newState);
                            closeLatch.countDown();
                        }
                    });

                    log.info("Connecting to {}:{} (conn #{})...", host, port, connIndex);
                    session.connectAndBind(host, port, bindParam);
                    activeSessionRef.set(session);
                    backoffSeconds = 1;
                    log.info("Bound to {}:{} (conn #{})", host, port, connIndex);

                    closeLatch.await();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (IOException e) {
                    Throwable cause = e.getCause();
                    String reason = cause != null ? cause.getMessage() : e.getMessage();
                    log.error("Bind to {}:{} failed: {}. Reconnecting in {}s", host, port, reason, backoffSeconds);
                } catch (Exception e) {
                    log.error("Unexpected error on {}:{}. Reconnecting in {}s", host, port, e.getMessage(), backoffSeconds);
                } finally {
                    activeSessionRef.set(null);
                    if (session != null) {
                        try { session.close(); } catch (Exception ignored) {}
                    }
                }

                if (running.get()) {
                    try { Thread.sleep(backoffSeconds * 1000L); } catch (InterruptedException e) { break; }
                    backoffSeconds = Math.min(backoffSeconds * 2, 30);
                }
            }
            log.info("Reconnect loop ended for {}:{}", host, port);
        }
    }
}
