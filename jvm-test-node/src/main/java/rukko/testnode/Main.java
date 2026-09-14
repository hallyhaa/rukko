package rukko.testnode;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A minimal Pekko node with a handful of test actors under /user:
 *
 *   echo   - replies with the exact String it received
 *   json   - replies with a JSON document describing what it received (text, sender path)
 *   silent - never replies (for timeout tests)
 *   slow   - replies with the received String after a 2 s delay
 *   bytes  - replies with the UTF-8 bytes of the received String as byte[] (serializer 4, unsupported by Rukko)
 *   counter - replies with how many messages it has received so far, as a decimal String
 *
 * Usage: Main [port]   (default 25552, system name "PekkoNode")
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 25552;
        Config config = ConfigFactory
                .parseString("pekko.remote.artery.canonical.port = " + port)
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("PekkoNode", config);
        system.actorOf(Props.create(Echo.class), "echo");
        system.actorOf(Props.create(Json.class), "json");
        system.actorOf(Props.create(Silent.class), "silent");
        system.actorOf(Props.create(Slow.class), "slow");
        system.actorOf(Props.create(BytesReply.class), "bytes");
        system.actorOf(Props.create(Counter.class), "counter");

        System.out.println("RUKKO_TEST_NODE_READY pekko://PekkoNode@127.0.0.1:" + port);
        System.out.flush();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            system.terminate();
            try {
                system.getWhenTerminated().toCompletableFuture().get();
            } catch (Exception ignored) {
            }
        }));
        system.getWhenTerminated().toCompletableFuture().get();
    }

    static abstract class LoggingActor extends AbstractActor {
        final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

        void logReceived(Object message) {
            log.info("{} received [{}] from [{}]", getSelf().path().name(), message, getSender().path());
        }
    }

    public static final class Echo extends LoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        logReceived(s);
                        getSender().tell(s, getSelf());
                    })
                    .matchAny(o -> log.warning("echo got unexpected {}", o.getClass()))
                    .build();
        }
    }

    public static final class Json extends LoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        logReceived(s);
                        String json = "{\"received\":" + quote(s)
                                + ",\"sender\":" + quote(getSender().path().toString())
                                + ",\"length\":" + s.length() + "}";
                        getSender().tell(json, getSelf());
                    })
                    .build();
        }

        private static String quote(String s) {
            StringBuilder sb = new StringBuilder("\"");
            for (char c : s.toCharArray()) {
                switch (c) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> {
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                    }
                }
            }
            return sb.append('"').toString();
        }
    }

    public static final class Silent extends LoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(this::logReceived).build();
        }
    }

    public static final class Slow extends LoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        logReceived(s);
                        var sender = getSender();
                        var self = getSelf();
                        getContext().getSystem().scheduler().scheduleOnce(
                                Duration.ofSeconds(2),
                                () -> sender.tell(s, self),
                                getContext().getDispatcher());
                    })
                    .build();
        }
    }

    public static final class BytesReply extends LoggingActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        logReceived(s);
                        getSender().tell(s.getBytes(StandardCharsets.UTF_8), getSelf());
                    })
                    .build();
        }
    }

    public static final class Counter extends LoggingActor {
        private long count = 0;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, s -> {
                        logReceived(s);
                        count++;
                        getSender().tell(Long.toString(count), getSelf());
                    })
                    .build();
        }
    }
}
