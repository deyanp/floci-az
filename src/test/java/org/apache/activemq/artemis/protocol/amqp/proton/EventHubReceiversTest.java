package org.apache.activemq.artemis.protocol.amqp.proton;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Azure lets at most five receivers read one partition of one consumer group, and refuses the
 * next with {@code amqp:resource-limit-exceeded}. A Service Bus queue keeps no such limit.
 */
class EventHubReceiversTest {

    private static final SimpleString PARTITION = SimpleString.of("eh1/ConsumerGroups/$Default/Partitions/0");

    private static URLClassLoader loader;
    private static Method attach;

    @BeforeAll
    static void loadFromPatchJar() throws Exception {
        loader = PatchJarLoader.open();
        Class<?> receivers = Class.forName(
                "org.apache.activemq.artemis.protocol.amqp.proton.EventHubReceivers", true, loader);
        attach = receivers.getMethod("attach", ActiveMQServer.class, SimpleString.class, Sender.class, Callable.class);
    }

    @AfterAll
    static void close() throws Exception {
        loader.close();
    }

    @Test
    @DisplayName("a partition of a consumer group takes five receivers and refuses the sixth")
    void refusesTheSixthReceiver() throws Exception {
        Partition partition = new Partition(true);
        for (int i = 0; i < 5; i++) {
            partition.attach("receiver-" + i);
        }

        ActiveMQAMQPException refused = assertThrows(ActiveMQAMQPException.class, () -> partition.attach("receiver-5"));

        assertEquals("amqp:resource-limit-exceeded", refused.getAmqpError().toString());
        assertEquals(5, partition.consumers.size(), "the refused receiver got a consumer");
    }

    @Test
    @DisplayName("a receiver whose link closed no longer takes a place")
    void aClosedReceiverFreesItsPlace() throws Exception {
        Partition partition = new Partition(true);
        Sender first = partition.attach("receiver-0");
        for (int i = 1; i < 5; i++) {
            partition.attach("receiver-" + i);
        }

        when(first.getRemoteState()).thenReturn(EndpointState.CLOSED);

        partition.attach("receiver-5");
    }

    @Test
    @DisplayName("a destructive queue, as Service Bus has, keeps no limit")
    void leavesOtherQueuesAlone() throws Exception {
        Partition queue = new Partition(false);

        for (int i = 0; i < 10; i++) {
            queue.attach("receiver-" + i);
        }
    }

    /** A queue on a broker of its own, whose consumers are the receivers it admitted. */
    private static final class Partition {

        final List<Consumer> consumers = new ArrayList<>();
        final ActiveMQServer server = mock(ActiveMQServer.class);

        Partition(boolean nonDestructive) {
            Queue queue = mock(Queue.class);
            when(queue.isNonDestructive()).thenReturn(nonDestructive);
            when(queue.getConsumers()).thenAnswer(call -> List.copyOf(consumers));
            when(server.locateQueue(PARTITION)).thenReturn(queue);
        }

        /** Attaches a receiver named {@code name}, which becomes a consumer once admitted. */
        Sender attach(String name) throws Exception {
            Sender link = mock(Sender.class);
            when(link.getName()).thenReturn(name);
            when(link.getLocalState()).thenReturn(EndpointState.ACTIVE);
            when(link.getRemoteState()).thenReturn(EndpointState.ACTIVE);
            ProtonServerSenderContext receiver = mock(ProtonServerSenderContext.class);
            when(receiver.getSender()).thenReturn(link);
            ServerConsumer consumer = mock(ServerConsumer.class);
            when(consumer.getProtocolContext()).thenReturn(receiver);
            Callable<ServerConsumer> create = () -> {
                consumers.add(consumer);
                return consumer;
            };
            try {
                attach.invoke(null, server, PARTITION, link, create);
            } catch (InvocationTargetException e) {
                throw (Exception) e.getCause();
            }
            return link;
        }
    }
}
