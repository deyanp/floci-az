package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.Map;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.qpid.proton.engine.Record;

/**
 * Sends the reply to a CBS or management request to the connection that sent the request, and to
 * no other connection.
 *
 * <p>Azure answers a request on {@code $cbs} or {@code $management} on the connection that sent
 * it. Here the reply goes back to the request address, and that address is MULTICAST: each client
 * that attaches a receiver to it gets a subscription queue of its own, so without a filter every
 * client receives every reply. A client that matches replies by correlation id drops the strays.
 * A client that takes the next reply as its own reads another client's reply, or gets one on a
 * link it has already detached and loses the connection. The Rust SDK does both, and it uses the
 * same {@code reply-to} on every connection, so routing by {@code reply-to} cannot tell two
 * clients apart.
 *
 * <p>So the broker stamps each request with the id of the connection that sent it, each
 * responder copies the stamp onto its reply, and a subscription on a reply address matches only
 * the stamp of its own connection.
 */
public final class AmqpReplyRouting {

   /**
    * Application property that carries the id of the requesting connection. The responders that
    * copy it are built into other jars and cannot see this constant, so the name is duplicated in
    * {@code ServiceBusCbsResponder} (which the Event Hubs management responder uses as well) and in
    * {@code ServiceBusPeekPlugin}.
    */
   public static final String CONNECTION_ID_PROPERTY = "floci_connection_id";

   /** Every CBS and management request names its operation. A reply never does. */
   private static final String OPERATION = "operation";

   /** Service Bus puts the entity path in front of {@code $management}; Event Hubs does not. */
   private static final String CBS_NODE = "$cbs";
   private static final String MANAGEMENT_NODE = "$management";

   private static final String CONNECTION_ID_KEY = AmqpReplyRouting.class.getName();

   private AmqpReplyRouting() {
   }

   /**
    * True for an address a client receives replies on: {@code $cbs}, {@code $management} or
    * {@code {entity}/$management}. The intercept addresses the responders read requests from are
    * not reply addresses.
    */
   public static boolean isReplyAddress(SimpleString address) {
      if (address == null) {
         return false;
      }
      String path = address.toString();
      String node = path.substring(path.lastIndexOf('/') + 1);
      return CBS_NODE.equals(node) || MANAGEMENT_NODE.equals(node);
   }

   /**
    * Stamps a request with the id of the connection that sent it.
    *
    * <p>Only a request is stamped. The responders send their replies to the same address, and a
    * reply keeps the stamp it copied from its request. A stamp the client set itself is
    * overwritten, so a client cannot ask for the replies of another connection.
    *
    * @param linkAddress the target address of the link the message arrived on; null on an
    *                    anonymous relay link, where the message carries its own address
    */
   public static void stampRequest(AMQPConnectionContext connection, SimpleString linkAddress, Message message) {
      SimpleString address = linkAddress != null ? linkAddress : message.getAddressSimpleString();
      if (!isReplyAddress(address) || !(message instanceof AMQPMessage)
          || message.getObjectProperty(OPERATION) == null) {
         return;
      }
      message.putStringProperty(CONNECTION_ID_PROPERTY, connectionId(connection));
      // An AMQP message is sent from its encoded form, so the responder would not see a property
      // set here unless the message is encoded again.
      message.reencode();
   }

   /** Copies the stamp of a request onto the application properties of its reply. */
   public static void copyStamp(Message request, Map<String, Object> replyProperties) {
      Object connectionId = request.getObjectProperty(CONNECTION_ID_PROPERTY);
      if (connectionId != null) {
         replyProperties.put(CONNECTION_ID_PROPERTY, connectionId.toString());
      }
   }

   /**
    * The selector of a subscription on {@code address}. On a reply address it matches only the
    * replies stamped with this connection's id, and the client's own selector, if it sent one, is
    * kept in parentheses so that an {@code OR} in it cannot widen the match. On any other address
    * the client's selector is returned unchanged.
    */
   public static String replySelector(AMQPConnectionContext connection, SimpleString address, String selector) {
      if (!isReplyAddress(address)) {
         return selector;
      }
      String ownReplies = CONNECTION_ID_PROPERTY + " = '" + connectionId(connection) + "'";
      return selector == null || selector.isBlank() ? ownReplies : "(" + selector + ") AND " + ownReplies;
   }

   /**
    * The id of an AMQP connection, made the first time it is asked for and kept with the
    * connection.
    *
    * <p>It is not the connection id Artemis uses. That is the Netty channel id, and its string
    * form is the short one, which Netty documents as not globally unique. A random UUID is unique.
    * Only the connection's own event loop calls this (the attach of a link and the delivery of a
    * message both run there), so the check and the set need no lock.
    */
   static String connectionId(AMQPConnectionContext connection) {
      Record attachments = connection.getHandler().getConnection().attachments();
      String id = attachments.get(CONNECTION_ID_KEY, String.class);
      if (id == null) {
         id = UUID.randomUUID().toString();
         attachments.set(CONNECTION_ID_KEY, String.class, id);
      }
      return id;
   }
}
