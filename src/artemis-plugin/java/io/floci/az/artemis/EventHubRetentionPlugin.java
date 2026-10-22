package io.floci.az.artemis;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Removes each event from its consumer group queues once its hub's retention has passed.
 *
 * <p>In Event Hubs a read never removes an event. An event stays until its hub's retention ends,
 * and then it is gone for every reader. The consumer group queues are non-destructive, so a read
 * leaves the event where it is, and this plugin is what removes it: every second it deletes from
 * each queue the events enqueued longer ago than the queue's retention.
 *
 * <p>The age is the enqueued time that {@code EventHubPartitionPlugin} stamps on every event. Artemis
 * expiry would also remove events on time, but it writes an {@code absolute-expiry-time} into
 * each event, which Azure does not send.
 *
 * <p>The broker config passes each consumer group queue with its hub's retention in milliseconds,
 * in the {@value #RETENTION_PROPERTY} property: {@code queue=millis,queue=millis}.
 */
public final class EventHubRetentionPlugin implements ActiveMQServerPlugin {

    private static final String RETENTION_PROPERTY = "retention";
    /** How often the plugin looks for events whose retention has passed. */
    private static final long SWEEP_INTERVAL_MS = 1_000;

    private static final System.Logger LOG =
            System.getLogger(EventHubRetentionPlugin.class.getName());

    private final Map<SimpleString, Long> retentionByQueue = new HashMap<>();
    private ScheduledExecutorService sweeper;

    @Override
    public void init(Map<String, String> properties) {
        String spec = properties.get(RETENTION_PROPERTY);
        if (spec == null || spec.isBlank()) {
            return;
        }
        for (String entry : spec.split(",")) {
            int separator = entry.lastIndexOf('=');
            if (separator <= 0) {
                LOG.log(System.Logger.Level.WARNING, "Ignoring malformed retention entry '" + entry + "'");
                continue;
            }
            try {
                retentionByQueue.put(SimpleString.of(entry.substring(0, separator).trim()),
                        Long.parseLong(entry.substring(separator + 1).trim()));
            } catch (NumberFormatException e) {
                LOG.log(System.Logger.Level.WARNING, "Ignoring malformed retention entry '" + entry + "'");
            }
        }
    }

    @Override
    public void registered(ActiveMQServer server) {
        if (retentionByQueue.isEmpty()) {
            return;
        }
        sweeper = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "eventhub-retention");
            thread.setDaemon(true);
            return thread;
        });
        sweeper.scheduleWithFixedDelay(() -> sweep(server), SWEEP_INTERVAL_MS, SWEEP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unregistered(ActiveMQServer server) {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
    }

    /** Deletes, from every consumer group queue, the events older than the queue's retention. */
    void sweep(ActiveMQServer server) {
        long now = System.currentTimeMillis();
        retentionByQueue.forEach((name, retentionMs) -> {
            Queue queue = server.locateQueue(name);
            if (queue == null) {
                return;
            }
            try {
                deleteExpired(server, queue, FilterImpl.createFilter(
                        EventHubPartitionPlugin.ENQUEUED_TIME_PROPERTY + " < " + (now - retentionMs)));
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Could not remove expired events from " + name, e);
            }
        });
    }

    /**
     * Deletes the events at the head of the queue that {@code expired} matches, and stops at the
     * first one it does not match.
     *
     * <p>A queue holds its events in enqueued order, so the expired ones are at its head, and a
     * sweep reads only the events it deletes plus one. {@code Queue.deleteMatchingReferences} would
     * read every event in the queue, under the queue's lock, every second.
     *
     * <p>Each event is removed and then acknowledged as killed, in one transaction, as
     * {@code deleteMatchingReferences} does it: a non-destructive queue ignores a normal
     * acknowledgement, so only a killed one lets the event go.
     */
    private static void deleteExpired(ActiveMQServer server, Queue queue, Filter expired) throws Exception {
        Transaction tx = new TransactionImpl(server.getStorageManager());
        int deleted = 0;
        MessageReference head = queue.peekFirstMessage();
        while (head != null && expired.match(head.getMessage())) {
            MessageReference removed = queue.removeReferenceWithID(head.getMessage().getMessageID());
            if (removed == null) {
                break;
            }
            queue.acknowledge(tx, removed, AckReason.KILLED, null, false);
            deleted++;
            head = queue.peekFirstMessage();
        }
        if (deleted > 0) {
            tx.commit();
        }
    }
}
