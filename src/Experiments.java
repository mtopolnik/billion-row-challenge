import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Experiments {
    public static void main(String[] args) {
        long a = 0x8877665544332211L;
        System.out.println(longToString(a));
        long matchBits = semicolonMatchBits(a);
        System.out.println(longToString(matchBits));
        System.out.printf("%x\n", matchBits);
        int nameLen = nameLen(matchBits);
        System.out.println(nameLen);
        System.out.printf("%x\n", maskWord(a, matchBits));
        long lastWordMask = (long) nameLen << 60 >> 63;
        System.out.printf("%x\n", lastWordMask);
    }

    private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
    private static final long BROADCAST_0x01 = 0x0101010101010101L;
    private static final long BROADCAST_0x80 = 0x8080808080808080L;

    private static long semicolonMatchBits(long word) {
        long diff = word ^ BROADCAST_SEMICOLON;
        return (diff - BROADCAST_0x01) & (~diff & BROADCAST_0x80);
    }
    private static long maskWord(long word, long matchBits) {
        long mask = matchBits ^ (matchBits - 1);
        return word & mask;
    }

    private static int nameLen(long separator) {
        return (Long.numberOfTrailingZeros(separator) >>> 3);
    }

    static String longToString(long word) {
        final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
        buf.clear();
        buf.putLong(word);
        return new String(buf.array(), StandardCharsets.UTF_8);
    }
}
