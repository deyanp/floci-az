package org.apache.activemq.artemis.protocol.amqp.proton;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A reply on {@code $cbs} or {@code $management} goes back to an address every client receives
 * on. These cover both halves of keeping it to the client that asked: the broker stamps a request
 * with the connection that sent it, and a receiver on a reply address gets a selector that lets
 * through its own connection's replies and nothing else.
 */
class AmqpReplyRoutingTest {

    private static final String PROPERTY = "floci_connection_id";

    private static URLClassLoader loader;
    private static Method stampRequest;
    private static Method replySelector;
    private static Method copyStamp;
    private static Method connectionId;

    @BeforeAll
    static void loadFromPatchJar() throws Exception {
        loader = PatchJarLoader.open();
        Class<?> routing = Class.forName(
                "org.apache.activemq.artemis.protocol.amqp.proton.AmqpReplyRouting", true, loader);
        stampRequest = routing.getMethod(
                "stampRequest", AMQPConnectionContext.class, SimpleString.class, Message.class);
        replySelector = routing.getMethod(
                "replySelector", AMQPConnectionContext.class, SimpleString.class, String.class);
        copyStamp = routing.getMethod("copyStamp", Message.class, Map.class);
        connectionId = routing.getDeclaredMethod("connectionId", AMQPConnectionContext.class);
        connectionId.setAccessible(true);
    }

    @AfterAll
    static void close() throws Exception {
        loader.close();
    }

    // ── the stamp on a request ───────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"$cbs", "$management", "orders/$management", "orders/Subscriptions/audit/$management"})
    @DisplayName("a request to a reply address carries the id of its connection to the responder")
    void stampsRequests(String address) throws Exception {
        AMQPConnectionContext connection = connection();
        AMQPMessage request = message(Map.of("operation", "put-token"));

        stamp(connection, address, request);

        assertEquals(idOf(connection), stampOnTheWire(request));
    }

    /** The responders read their requests from intercept addresses, which no client receives on. */
    @ParameterizedTest
    @ValueSource(strings = {"$cbs-intercept", "$management-intercept", "eh1", "orders", "orders/$DeadLetterQueue"})
    @DisplayName("a message to any other address is left alone")
    void leavesOtherAddressesAlone(String address) throws Exception {
        AMQPMessage message = message(Map.of("operation", "put-token"));

        stamp(connection(), address, message);

        assertNull(stampOnTheWire(message));
    }

    /**
     * The responder sends its reply to the same address as the request, over a connection of its
     * own. Stamping it again would address it to the responder.
     */
    @Test
    @DisplayName("a reply keeps the stamp it copied from its request")
    void leavesRepliesAlone() throws Exception {
        AMQPMessage reply = message(Map.of("status-code", 202, PROPERTY, "requesting-connection"));

        stamp(connection(), "$cbs", reply);

        assertEquals("requesting-connection", stampOnTheWire(reply));
    }

    @Test
    @DisplayName("a stamp a client set itself is replaced by its real connection id")
    void overwritesForgedStamps() throws Exception {
        AMQPConnectionContext connection = connection();
        AMQPMessage request = message(Map.of("operation", "READ", PROPERTY, "another-connection"));

        stamp(connection, "$management", request);

        assertEquals(idOf(connection), stampOnTheWire(request));
    }

    @Test
    @DisplayName("on an anonymous relay link the address the message names decides")
    void readsTheMessageAddressOnAnAnonymousLink() throws Exception {
        AMQPConnectionContext connection = connection();
        AMQPMessage request = message(Map.of("operation", "put-token"));
        request.setAddress("$cbs");

        stampRequest.invoke(null, connection, null, request);

        assertEquals(idOf(connection), stampOnTheWire(request));
    }

    @Test
    @DisplayName("a connection keeps one id, and no two connections share one")
    void connectionIdsAreStableAndUnique() throws Exception {
        AMQPConnectionContext first = connection();
        AMQPConnectionContext second = connection();

        assertEquals(idOf(first), idOf(first));
        assertNotEquals(idOf(first), idOf(second));
    }

    // ── the selector of a receiver ───────────────────────────────────────────

    /**
     * The point of the whole change, checked with the broker's own filter: two connections
     * receive on the same address, and each receiver lets through only the replies to its own
     * requests. A reply with no stamp at all reaches nobody.
     */
    @ParameterizedTest
    @ValueSource(strings = {"$cbs", "$management", "orders/$management"})
    @DisplayName("a receiver on a reply address takes its own connection's replies and no other")
    void receiverTakesOnlyItsOwnReplies(String address) throws Exception {
        AMQPConnectionContext mine = connection();
        AMQPConnectionContext theirs = connection();

        Filter filter = FilterImpl.createFilter(selector(mine, address, null));

        assertTrue(filter.match(message(Map.of("status-code", 202, PROPERTY, idOf(mine)))));
        assertFalse(filter.match(message(Map.of("status-code", 202, PROPERTY, idOf(theirs)))));
        assertFalse(filter.match(message(Map.of("status-code", 202))));
    }

    @Test
    @DisplayName("a client's own selector is kept, and an OR in it cannot widen the match")
    void keepsTheClientSelector() throws Exception {
        AMQPConnectionContext mine = connection();
        AMQPConnectionContext theirs = connection();

        String selector = selector(mine, "$cbs", "kind = 'a' OR kind = 'b'");
        Filter filter = FilterImpl.createFilter(selector);

        assertEquals("(kind = 'a' OR kind = 'b') AND " + PROPERTY + " = '" + idOf(mine) + "'", selector);
        assertTrue(filter.match(message(Map.of("kind", "b", PROPERTY, idOf(mine)))));
        assertFalse(filter.match(message(Map.of("kind", "a", PROPERTY, idOf(theirs)))));
        assertFalse(filter.match(message(Map.of("kind", "c", PROPERTY, idOf(mine)))));
    }

    @Test
    @DisplayName("a receiver anywhere else keeps the selector the client sent")
    void leavesOtherSelectorsAlone() throws Exception {
        AMQPConnectionContext connection = connection();

        assertEquals("floci_offset > -1",
                selector(connection, "eh1/ConsumerGroups/$Default/Partitions/0", "floci_offset > -1"));
        assertNull(selector(connection, "orders", null));
        assertNull(selector(connection, "$cbs-intercept", null));
    }

    // ── the copy on a reply ──────────────────────────────────────────────────

    @Test
    @DisplayName("a reply copies the stamp of its request, and only when there is one")
    void copiesTheStampOntoReplies() throws Exception {
        Map<String, Object> replyProperties = new HashMap<>();
        copyStamp.invoke(null, message(Map.of("operation", "READ", PROPERTY, "requesting-connection")),
                replyProperties);
        assertEquals(Map.of(PROPERTY, "requesting-connection"), replyProperties);

        Map<String, Object> unstamped = new HashMap<>();
        copyStamp.invoke(null, message(Map.of("operation", "READ")), unstamped);
        assertTrue(unstamped.isEmpty());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** A broker-side connection. The patch keeps its id with the proton connection under it. */
    private static AMQPConnectionContext connection() {
        AMQPConnectionContext connection = mock(AMQPConnectionContext.class, RETURNS_DEEP_STUBS);
        when(connection.getHandler().getConnection()).thenReturn(Proton.connection());
        return connection;
    }

    private static String idOf(AMQPConnectionContext connection) throws Exception {
        return (String) invoke(connectionId, connection);
    }

    private static void stamp(AMQPConnectionContext connection, String address, Message message)
            throws Exception {
        invoke(stampRequest, connection, SimpleString.of(address), message);
    }

    private static String selector(AMQPConnectionContext connection, String address, String selector)
            throws Exception {
        return (String) invoke(replySelector, connection, SimpleString.of(address), selector);
    }

    private static Object invoke(Method method, Object... args) throws Exception {
        try {
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private static AMQPStandardMessage message(Map<String, Object> applicationProperties) {
        var message = Proton.message();
        message.setMessageId("request-1");
        message.setApplicationProperties(new ApplicationProperties(applicationProperties));
        byte[] buffer = new byte[1024];
        int size = message.encode(buffer, 0, buffer.length);
        return new AMQPStandardMessage(0, Arrays.copyOf(buffer, size), null);
    }

    /** The stamp as the responder reads it: from the bytes the broker sends, not the broker's view. */
    private static Object stampOnTheWire(AMQPMessage message) {
        ReadableBuffer data = message.getSendBuffer(1, null);
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        var decoded = Proton.message();
        decoded.decode(bytes, 0, bytes.length);
        return decoded.getApplicationProperties().getValue().get(PROPERTY);
    }
}
