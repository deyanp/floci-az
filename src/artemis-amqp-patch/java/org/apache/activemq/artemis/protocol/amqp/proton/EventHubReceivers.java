package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;

/**
 * Admits the receivers of an Event Hubs partition in a consumer group, within the limits Azure sets.
 *
 * <p>Azure lets at most five receivers read one partition of one consumer group. The next one is
 * refused with {@code amqp:resource-limit-exceeded}, which the SDKs report as quota exceeded.
 *
 * <p>A partition of a consumer group is one consumer group queue, and its receivers are that queue's
 * consumers, so the limit reads the broker's own state and keeps none of its own. Every consumer
 * group queue is non-destructive (see {@code ArtemisConfigGenerator}). That is how a queue is told
 * apart from any other, a Service Bus queue included, which keeps no such limit.
 */
public final class EventHubReceivers {

   /** Azure's limit of receivers per partition per consumer group. */
   static final int MAX_RECEIVERS = 5;
   static final Symbol RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf("amqp:resource-limit-exceeded");

   /** Admits one receiver at a time, so that two attaching at once cannot both take the last place. */
   private static final Object ADMISSION = new Object();

   private EventHubReceivers() {
   }

   /**
    * Creates the consumer of a receiver that attaches to {@code queueName}, once Azure's limits
    * admit it. A queue that is not a consumer group queue admits every receiver.
    */
   public static ServerConsumer attach(ActiveMQServer server, SimpleString queueName, Sender link,
                                       Callable<ServerConsumer> create) throws Exception {
      Queue queue = server.locateQueue(queueName);
      if (queue == null || !queue.isNonDestructive()) {
         return create.call();
      }
      synchronized (ADMISSION) {
         admit(open(queue.getConsumers()), link);
         return create.call();
      }
   }

   /** Refuses {@code link} when the receivers already open on its partition leave it no place. */
   static void admit(List<ProtonServerSenderContext> receivers, Sender link) throws ActiveMQAMQPException {
      if (receivers.size() >= MAX_RECEIVERS) {
         throw new ActiveMQAMQPException(RESOURCE_LIMIT_EXCEEDED,
            "Exceeded the maximum number of allowed receivers per partition in a consumer group which is "
               + MAX_RECEIVERS + ". Receiver '" + link.getName() + "' is refused. List of connected receivers - "
               + receivers.stream().map(receiver -> receiver.getSender().getName()).collect(Collectors.joining(", "))
               + ".",
            ActiveMQExceptionType.GENERIC_EXCEPTION);
      }
   }

   /**
    * The receivers of a queue's consumers whose links are still open. A link stays a consumer for a
    * moment after it closes, until its connection removes it, and no longer counts as a receiver then.
    */
   static List<ProtonServerSenderContext> open(Collection<Consumer> consumers) {
      List<ProtonServerSenderContext> receivers = new ArrayList<>();
      for (Consumer consumer : consumers) {
         if (consumer instanceof ServerConsumer server
            && server.getProtocolContext() instanceof ProtonServerSenderContext receiver
            && receiver.getSender().getLocalState() != EndpointState.CLOSED
            && receiver.getSender().getRemoteState() != EndpointState.CLOSED) {
            receivers.add(receiver);
         }
      }
      return receivers;
   }
}
