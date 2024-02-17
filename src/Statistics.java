import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

public class Statistics {
    static final String MEASUREMENTS_TXT = "../1brc/measurements-500m.txt";

    public static void main(String[] args) throws Exception {
        var in = new BufferedReader(new FileReader(MEASUREMENTS_TXT));
        long shortCount = 0;
        long longCount = 0;
        long totalCount = 0;
        long threshold = 32; // up to (including) this many bytes is short
        long lineCount = 0;
        while (true) {
            var line = in.readLine();
            if (line == null) {
                break;
            }
            if (++lineCount % 10_000_000 == 0) {
                System.out.printf("%,d%n", lineCount);
            }
            var nameLen = line.indexOf(';');
            totalCount += nameLen;
            shortCount += Math.min(threshold, nameLen);
            longCount += Math.max(0, nameLen - threshold + 1);
        }
        System.out.printf("Short: %.2f%%, long: %.2f%%%n",
                100.0 * shortCount / totalCount, 100.0 * longCount / totalCount);
    }

    public static void x(String[] args) throws Exception {
        var in = new BufferedReader(new FileReader(MEASUREMENTS_TXT));
        var sizes = new int[100];
        var lineCount = 0;
        while (true) {
            var line = in.readLine();
            if (line == null) {
                break;
            }
            if (++lineCount % 10_000_000 == 0) {
                System.out.printf("%,d%n", lineCount);
            }
            var nameLen = line.indexOf(';');
            sizes[nameLen] += 1; //nameLen;
        }
        var longest = Arrays.stream(sizes).max().orElse(0);
        var scale = longest / 100;
        for (int i = 0; i < sizes.length; i++) {
            System.out.printf("%2d -> %,d%n", i, sizes[i]);
//            System.out.printf("%2d -> %s%n", i * 4, "X".repeat(sizes[i] / scale));
        }
    }
}

// Baseline:
//
// 0 ->
// 4 -> XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
// 8 -> XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//12 -> XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//16 -> XXXXXXXXXXXXXXX
//20 ->
//24 -> XXXXXX
//28 ->
//32 ->
//36 ->

// New:
//
// 0 -> XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
// 4 -> XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
// 8 -> XXXXXXXXXXXXXXXXXX
//12 -> XXXXXXXXXXXXXXXXXX
//16 -> XXXXXXXXXXXXXXXX
//20 -> XXXXXXXXXXXXXXXX
//24 -> XXXXXXXXXXXXXXXXX
//28 -> XXXXXXXXXXXXXXXXXX
//32 -> XXXXXXXXXXXXXXXX
//36 -> XXXXXXXXXXXXXXXXX
//40 -> XXXXXXXXXXXXXXXXXX
//44 -> XXXXXXXXXXXXXXXXX
//48 -> XXXXXXXXXXXXXXXXXXX
//52 -> XXXXXXXXXXXXXXX
//56 -> XXXXXXXXXXXXXXX
//60 -> XXXXXXXXXXXXXXXXXXX
//64 -> XXXXXXXXXXXXX
//68 -> XXXXXXXXXXXXXXXXXXX
//72 -> XXXXXXXXXXXXX
//76 -> XXXXXXXXXXXXXXXXXXXXXX
//80 -> XXXXXXXXXXXXXXXXX
//84 -> XXXXXXXXXXXXXXXXXXX
//88 -> XXXXXXXXXXXXXXXXX
//92 -> XXXXXXXXXXXXXXXXXX
//96 -> XXXXXXXXXXXXXXXX
