package io.floci.az.artemis;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Assigns each message an Event Hubs partition as it is routed.
 *
 * <p>Artemis has no notion of partitions, so the emulated ones are ordinary queues selected by a
 * filter. This plugin supplies what that filter matches on: it stamps {@value #PARTITION_PROPERTY}
 * with the partition the message belongs to, and the generated broker.xml gives each partition a
 * divert filtered on that value. A message therefore reaches exactly one partition per consumer
 * group — every group sees the whole stream, each event in one partition of it.
 *
 * <p>Assignment follows the Event Hubs rules, in order of precedence:
 * <ol>
 *   <li>an explicitly addressed partition id, honoured as given;</li>
 *   <li>a partition key, hashed so the same key always lands in the same partition;</li>
 *   <li>neither — round-robin across the hub's partitions.</li>
 * </ol>
 *
 * <p>The hash is {@link String#hashCode()}, which the Java language specification pins, so the
 * mapping is stable across runs and JVMs. It deliberately does not reproduce Azure's own hash:
 * that is undocumented, and client code depends on the guarantee (same key, same partition;
 * ordering within a partition) rather than on which index a key lands in.
 */
public final class EventHubPartitionPlugin implements ActiveMQServerPlugin {

    /** Property the generated partition diverts filter on. */
    public static final String PARTITION_PROPERTY = "floci_partition";
    /** Set by senders that pin a message to a partition. */
    private static final String PARTITION_ID_ANNOTATION = "x-opt-partition-id";
    /** Set by senders that supply a partition key. */
    private static final String PARTITION_KEY_ANNOTATION = "x-opt-partition-key";
    /** Names the Event Hubs start-position selectors reference. */
    private static final String OFFSET_ANNOTATION = "amqp.annotation.x-opt-offset";
    private static final String SEQUENCE_NUMBER_ANNOTATION = "amqp.annotation.x-opt-sequence-number";
    private static final String ENQUEUED_TIME_ANNOTATION = "amqp.annotation.x-opt-enqueued-time";
    /** Plugin property: "eh1:4,eh2:2". */
    private static final String ENTITIES_PROPERTY = "entities";

    private static final System.Logger LOG =
            System.getLogger(EventHubPartitionPlugin.class.getName());

    private final Map<String, Integer> partitionCounts = new HashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    @Override
    public void init(Map<String, String> properties) {
        String entities = properties.get(ENTITIES_PROPERTY);
        if (entities == null || entities.isBlank()) {
            return;
        }
        for (String token : entities.split(",")) {
            String[] parts = token.trim().split(":", 2);
            if (parts.length != 2 || parts[0].isBlank()) {
                continue;
            }
            try {
                partitionCounts.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
            } catch (NumberFormatException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "Ignoring malformed partition count in '" + token + "'");
            }
        }
    }

    /**
     * Stamps the partition before the message is routed.
     *
     * <p>It has to be {@code beforeSend} rather than the more obvious {@code beforeMessageRoute}.
     * Despite the name, {@code beforeMessageRoute} fires inside {@code PostOfficeImpl.route}
     * <em>after</em> the bindings have already been resolved — diverts and their filters are
     * evaluated in {@code simpleRoute} further up, so a property set there is too late to steer
     * routing and the partition diverts never match. {@code beforeSend} runs in
     * {@code ServerSessionImpl.doSend} before {@code postOffice.route} is called at all.
     */
    @Override
    public void beforeSend(ServerSession session, Transaction tx, Message message,
                           boolean direct, boolean noAutoCreateQueue) {
        String address = message.getAddress();
        if (address == null) {
            return;
        }
        int partitionCount = partitionCountFor(address);
        // Even a single-partition hub is stamped: its divert filters on this like any other.
        int partition = partitionCount <= 1 ? 0 : choosePartition(message, address, partitionCount);
        // Stamped as a string, and compared as one by the generated divert filters. That mirrors
        // the CBS divert, the one filter on an AMQP application property already known to work
        // here.
        message.putStringProperty(PARTITION_PROPERTY, Integer.toString(partition));
        stampStreamPosition(message, address, partition);
        // An AMQP message serves its properties from its encoded form, so a property set here is
        // invisible to the divert filters until the message is re-encoded.
        message.reencode();
    }

    /**
     * Stamps the stream position a consumer's start position is expressed against.
     *
     * <p>Every Event Hubs start position becomes an AMQP selector over an annotation — even
     * "earliest", which is sent as {@code amqp.annotation.x-opt-offset > '-1'}. Artemis reads
     * annotations in filters only under its own {@code m.} prefix, so those selectors fall through
     * to an ordinary property lookup by the full name. Naming the properties exactly as the
     * selectors reference them is therefore what makes start positions work at all.
     *
     * <p>The values are strings because the selectors quote their operands, so the comparison is
     * lexicographic. That is exact for enqueued time, whose millisecond stamps are all the same
     * width, and for offsets only while they are — a consumer resuming from a specific offset
     * across a digit boundary is a known limitation of this emulation.
     */
    private void stampStreamPosition(Message message, String address, int partition) {
        long sequence = sequences
                .computeIfAbsent(address + "#" + partition, k -> new AtomicLong())
                .getAndIncrement();
        message.putStringProperty(OFFSET_ANNOTATION, Long.toString(sequence));
        message.putStringProperty(SEQUENCE_NUMBER_ANNOTATION, Long.toString(sequence));
        message.putStringProperty(ENQUEUED_TIME_ANNOTATION, Long.toString(System.currentTimeMillis()));
    }

    private int choosePartition(Message message, String address, int partitionCount) {
        Object pinned = message.getObjectProperty(PARTITION_ID_ANNOTATION);
        if (pinned != null) {
            try {
                int id = Integer.parseInt(pinned.toString().trim());
                if (id >= 0 && id < partitionCount) {
                    return id;
                }
                LOG.log(System.Logger.Level.WARNING,
                        "Partition id " + id + " is outside 0.." + (partitionCount - 1)
                        + " for " + address + "; falling back to the partition key");
            } catch (NumberFormatException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "Ignoring non-numeric partition id '" + pinned + "' on " + address);
            }
        }

        Object key = message.getObjectProperty(PARTITION_KEY_ANNOTATION);
        if (key != null) {
            return Math.floorMod(key.toString().hashCode(), partitionCount);
        }

        return Math.floorMod(
                roundRobin.computeIfAbsent(address, a -> new AtomicInteger()).getAndIncrement(),
                partitionCount);
    }

    /**
     * The entity is the last segment of the address the sender used
     * ({@code amqps://host/eh1} → {@code eh1}); hubs are configured by bare name.
     */
    private int partitionCountFor(String address) {
        int slash = address.lastIndexOf('/');
        String entity = slash >= 0 ? address.substring(slash + 1) : address;
        return partitionCounts.getOrDefault(entity, 1);
    }
}
