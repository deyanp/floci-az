package io.floci.az.services.servicebus;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The broker stamps each CBS and management request with the id of the connection that sent it,
 * and a client receives only the replies that carry its own id back. A responder that drops the
 * stamp sends a reply nobody receives. Every request gets a reply, and an operation floci-az does
 * not implement gets 501.
 */
class ServiceBusCbsResponderTest {

    @Test
    @DisplayName("a reply carries the id of the requesting connection back")
    void copiesTheConnectionId() {
        Message request = Message.Factory.create();
        request.setApplicationProperties(new ApplicationProperties(Map.of(
                "operation", "put-token",
                ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, "requesting-connection")));
        Map<String, Object> reply = new HashMap<>(Map.of("status-code", 202));

        ServiceBusCbsResponder.copyConnectionId(request, reply);

        assertEquals("requesting-connection", reply.get(ServiceBusCbsResponder.CONNECTION_ID_PROPERTY));
        assertEquals(202, reply.get("status-code"));
    }

    @Test
    @DisplayName("a request without a stamp adds nothing to the reply")
    void copiesNothingWithoutAStamp() {
        Message withoutProperties = Message.Factory.create();
        Message withoutStamp = Message.Factory.create();
        withoutStamp.setApplicationProperties(new ApplicationProperties(Map.of("operation", "put-token")));

        for (Message request : new Message[]{withoutProperties, withoutStamp}) {
            Map<String, Object> reply = new HashMap<>();
            ServiceBusCbsResponder.copyConnectionId(request, reply);
            assertTrue(reply.isEmpty());
        }
    }

    @Test
    @DisplayName("a put-token gets 202, correlated to its request and carrying its stamp")
    void acceptsAPutToken() {
        Message reply = ServiceBusCbsResponder.replyTo(request("put-token"));

        assertEquals("request-1", reply.getCorrelationId());
        assertEquals(Map.of("status-code", 202, "status-description", "Accepted",
                ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, "requesting-connection"), properties(reply));
    }

    @Test
    @DisplayName("any other CBS operation gets 501, which names the operation")
    void answersOtherOperationsWithNotImplemented() {
        Message reply = ServiceBusCbsResponder.replyTo(request("delete-token"));

        assertEquals("request-1", reply.getCorrelationId());
        assertEquals(Map.of("status-code", 501,
                "status-description", "floci-az does not implement the operation 'delete-token'",
                ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, "requesting-connection"), properties(reply));
    }

    /** The reply names the operation the client chose, so it can outgrow any fixed buffer. */
    @Test
    @DisplayName("a reply that names a long operation still encodes whole")
    void encodesALongReply() {
        String operation = "x".repeat(5_000);

        byte[] encoded = ServiceBusCbsResponder.encode(ServiceBusCbsResponder.replyTo(request(operation)));
        Message decoded = Message.Factory.create();
        decoded.decode(encoded, 0, encoded.length);

        assertEquals("floci-az does not implement the operation '" + operation + "'",
                properties(decoded).get("status-description"));
    }

    /**
     * The property is set and filtered on in the patched AMQP jar and copied here and in the
     * broker plugin, so its name is spelled in three jars. A spelling that drifts breaks no build:
     * the reply is delivered to no one, and the client waits until it times out.
     */
    @Test
    @DisplayName("the broker and every responder name the connection id property alike")
    void everyJarNamesThePropertyAlike() throws Exception {
        try (URLClassLoader patch = jarLoader(ArtemisPatchVersions.ARTEMIS_AMQP_PATCH_RESOURCE);
             URLClassLoader plugin = jarLoader(ServiceBusNamespaceManager.ARTEMIS_EXTENSION_RESOURCE)) {
            assertEquals(ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, constant(patch,
                    "org.apache.activemq.artemis.protocol.amqp.proton.AmqpReplyRouting",
                    "CONNECTION_ID_PROPERTY"));
            assertEquals(ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, constant(plugin,
                    "io.floci.az.artemis.ServiceBusPeekPlugin", "CONNECTION_ID_PROPERTY"));
        }
    }

    /** A request as the responder receives it: stamped with the id of the connection that sent it. */
    private static Message request(String operation) {
        Message request = Message.Factory.create();
        request.setMessageId("request-1");
        request.setApplicationProperties(new ApplicationProperties(Map.of(
                "operation", operation,
                ServiceBusCbsResponder.CONNECTION_ID_PROPERTY, "requesting-connection")));
        return request;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Message reply) {
        return reply.getApplicationProperties().getValue();
    }

    private URLClassLoader jarLoader(String resource) {
        URL jar = getClass().getResource(resource);
        assertNotNull(jar, "Jar not found: " + resource + "; the build writes it, so run the build");
        return new URLClassLoader(new URL[]{jar}, getClass().getClassLoader());
    }

    private static Object constant(ClassLoader loader, String className, String fieldName) throws Exception {
        Field field = Class.forName(className, true, loader).getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }
}
