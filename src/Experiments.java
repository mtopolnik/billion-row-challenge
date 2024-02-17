import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Experiments {
    public static void main(String[] args) {
        /*
00000000  44 75 6e 65 64 69 6e 3b  38 2e 30 0a 42 6f 75 61  |Dunedin;8.0.Boua|
00000010  6b c3 a9 3b 33 39 2e 37  0a 4c 61 67 6f 73 3b 32  |k..;39.7.Lagos;2|
00000020  37 2e 39 0a 44 61 6d 61  73 63 75 73 3b 32 36 2e  |7.9.Damascus;26.|
 */

        long nameWord0 = 0x3b_6e_69_64_65_6e_75_44L;
        long nameWord1 = 0x61_75_6f_42_0a_30_2e_38L;
        long matchBits0 = semicolonMatchBits(nameWord0);
        long matchBits1 = semicolonMatchBits(nameWord1);
        System.out.format("match bits 0 %x\n", matchBits0);
        System.out.format("match bits 1 %x\n", matchBits1);

        int temperature;
        long lastNameWord;
        if ((matchBits0 | matchBits1) != 0) {
            long tempWord = nameWord1;
            int nameLen0 = nameLen(matchBits0);
            int nameLen1 = nameLen(matchBits1);
            nameWord0 = maskWord(nameWord0, matchBits0);
            nameWord1 = maskWord(nameWord1, matchBits1) & ~broadcastBit3(nameLen1);
            nameLen1 &= 0b111;
            int nameLen = nameLen0 + nameLen1;
            long lastWordMask = broadcastBit3(nameLen0);
            lastNameWord = (nameWord0 & lastWordMask) | (nameWord1 & ~lastWordMask);
            nameLen++;
            int dotPos = dotPos(tempWord);
            temperature = parseTemperature(tempWord, dotPos);

            System.out.printf("name word 0 '%s'\n", longToString(nameWord0));
            System.out.printf("name word 1 '%s'\n", longToString(nameWord1));
            System.out.println("name len 0 " + nameLen0);
            System.out.println("name len 1 " + nameLen1);
            System.out.format("last name word '%s'\n", longToString(lastNameWord));
            System.out.println("temperature " + temperature);
            System.out.println("name len " + nameLen);
        }
    }

    private static long broadcastBit3(long word) {
        return word << 60 >> 63;
    }

    private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
    private static final long BROADCAST_0x01 = 0x0101010101010101L;
    private static final long BROADCAST_0x80 = 0x8080808080808080L;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);
    private static final long DOT_BITS = 0x10101000;

    private static int dotPos(long word) {
        return Long.numberOfTrailingZeros(~word & DOT_BITS);
    }

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

    private static int parseTemperature(long numberBytes, int dotPos) {
        // numberBytes contains the number: X.X, -X.X, XX.X or -XX.X
        final long invNumberBytes = ~numberBytes;

        // Calculates the sign
        final long signed = (invNumberBytes << 59) >> 63;
        final int _28MinusDotPos = (dotPos ^ 0b11100);
        final long minusFilter = ~(signed & 0xFF);
        // Use the pre-calculated decimal position to adjust the values
        final long digits = ((numberBytes & minusFilter) << _28MinusDotPos) & 0x0F000F0F00L;

        // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        // And apply sign
        return (int) ((absValue + signed) ^ signed);
    }

    static String longToString(long word) {
        final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
        buf.clear();
        buf.putLong(word);
        return new String(buf.array(), StandardCharsets.UTF_8);
    }
}
