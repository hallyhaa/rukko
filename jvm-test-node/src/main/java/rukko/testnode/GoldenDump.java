package rukko.testnode;

import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.ExtendedActorSystem;
import org.apache.pekko.remote.UniqueAddress;
import org.apache.pekko.remote.artery.ActorSystemTerminating;
import org.apache.pekko.remote.artery.ActorSystemTerminatingAck;
import org.apache.pekko.remote.artery.EnvelopeBuffer;
import org.apache.pekko.remote.artery.HeaderBuilder;
import org.apache.pekko.remote.artery.OutboundHandshake;
import org.apache.pekko.remote.serialization.ArteryMessageSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Dumps envelope headers and control-message payloads exactly as Pekko 1.1.x encodes them,
 * so that they can be used as golden test vectors in Rukko (src/golden_tests.rs).
 *
 * Run with: mvn -q compile exec:java -Dexec.mainClass=rukko.testnode.GoldenDump
 */
public final class GoldenDump {

    public static void main(String[] args) throws Exception {
        var config = ConfigFactory
                .parseString("pekko.remote.artery.canonical.port = 0")
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("GoldenSys", config);
        try {
            ExtendedActorSystem ext = (ExtendedActorSystem) system;

            ActorRef sender = ext.provider().resolveActorRef("pekko://RukkoIT@127.0.0.1:4242/temp/_user_echo$a");
            ActorRef recipient = ext.provider().resolveActorRef("pekko://PekkoNode@127.0.0.1:25552/user/echo");

            // 1. A user String message, serializer 20 (primitive-string), empty manifest, literal refs
            dump("USER_STRING_ENVELOPE", envelope(42L, 20, sender, recipient, "", "Hello Artery!".getBytes(StandardCharsets.UTF_8)));

            // 2. Same message but with no sender (deadLetters): Pekko writes an empty literal
            dump("USER_STRING_NO_SENDER_ENVELOPE", envelopeNoSender(42L, 20, recipient, "", "Hello Artery!".getBytes(StandardCharsets.UTF_8)));

            // 3. Control messages: payload bytes and manifests from ArteryMessageSerializer (id 17)
            ArteryMessageSerializer ser = new ArteryMessageSerializer(ext);
            System.out.println("ARTERY_SERIALIZER_ID=" + ser.identifier());

            Address rukkoAddr = Address.apply("pekko", "RukkoIT", "127.0.0.1", 4242);
            Address pekkoAddr = Address.apply("pekko", "PekkoNode", "127.0.0.1", 25552);
            UniqueAddress rukkoUnique = new UniqueAddress(rukkoAddr, 42L);
            UniqueAddress pekkoUnique = new UniqueAddress(pekkoAddr, 7L);

            Object handshakeReq = new OutboundHandshake.HandshakeReq(rukkoUnique, pekkoAddr);
            Object handshakeRsp = new OutboundHandshake.HandshakeRsp(pekkoUnique);
            Object terminating = new ActorSystemTerminating(rukkoUnique);
            Object terminatingAck = new ActorSystemTerminatingAck(pekkoUnique);

            dumpControl(ser, "HANDSHAKE_REQ", handshakeReq);
            dumpControl(ser, "HANDSHAKE_RSP", handshakeRsp);
            dumpControl(ser, "ACTOR_SYSTEM_TERMINATING", terminating);
            dumpControl(ser, "ACTOR_SYSTEM_TERMINATING_ACK", terminatingAck);

            // 4. A complete HandshakeReq envelope as Pekko would frame it (sender/recipient are not set for control messages)
            dump("HANDSHAKE_REQ_ENVELOPE", envelopeNoRefs(42L, ser.identifier(), ser.manifest(handshakeReq), ser.toBinary(handshakeReq)));

            // 5. Large uid / negative uid edge case (Long.MIN_VALUE) with a long manifest
            dump("EDGE_ENVELOPE", envelope(Long.MIN_VALUE, -1, sender, recipient, "longlonglongliteralmanifest", new byte[0]));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get();
        }
    }

    private static void dumpControl(ArteryMessageSerializer ser, String name, Object msg) {
        System.out.println(name + "_MANIFEST=" + ser.manifest(msg));
        System.out.println(name + "_PAYLOAD=" + hex(ser.toBinary(msg)));
    }

    private static byte[] envelope(long uid, int serializer, ActorRef sender, ActorRef recipient, String manifest, byte[] payload) {
        HeaderBuilder h = HeaderBuilder.out();
        h.setVersion((byte) 0);
        h.setUid(uid);
        h.setSerializer(serializer);
        h.setSenderActorRef(sender);
        h.setRecipientActorRef(recipient);
        h.setManifest(manifest);
        return write(h, payload);
    }

    private static byte[] envelopeNoSender(long uid, int serializer, ActorRef recipient, String manifest, byte[] payload) {
        HeaderBuilder h = HeaderBuilder.out();
        h.setVersion((byte) 0);
        h.setUid(uid);
        h.setSerializer(serializer);
        h.setNoSender();
        h.setRecipientActorRef(recipient);
        h.setManifest(manifest);
        return write(h, payload);
    }

    private static byte[] envelopeNoRefs(long uid, int serializer, String manifest, byte[] payload) {
        HeaderBuilder h = HeaderBuilder.out();
        h.setVersion((byte) 0);
        h.setUid(uid);
        h.setSerializer(serializer);
        h.setNoSender();
        h.setNoRecipient();
        h.setManifest(manifest);
        return write(h, payload);
    }

    private static byte[] write(HeaderBuilder h, byte[] payload) {
        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        EnvelopeBuffer env = new EnvelopeBuffer(bb);
        env.writeHeader(h);
        bb.put(payload);
        bb.flip();
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
    }

    private static void dump(String name, byte[] bytes) {
        System.out.println(name + "=" + hex(bytes));
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
