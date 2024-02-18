import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

public class Statistics {
    static final String MEASUREMENTS_TXT = "measurements.txt";

    public static void main(String[] args) throws Exception {
        distribution();
//        branchPrediction();
    }

    public static void distribution() throws Exception {
        var in = new BufferedReader(new FileReader(MEASUREMENTS_TXT));
        var sizes = new int[25];
        var lineCount = 0;
        while (true) {
            var line = in.readLine();
            if (line == null) {
                break;
            }
            if (++lineCount % 10_000_000 == 0) {
                System.out.printf("%,d%n", lineCount);
            }
            // actual nameLen in 1BRC code includes the semicolon, this is deliberately one less,
            // so we get buckets that correspond to the condition nameLen > 8 and nameLen > 16
            var nameLen = line.indexOf(';');
            // use "+= nameLen" here to get byte distribution instead: for a randomly chosen name byte
            // in the input, how likely is it to find itself in a name of length n?
            sizes[nameLen / 4] += 1;
        }
        var longest = Arrays.stream(sizes).max().orElse(0);
        var scale = longest / 100;
        var shortCount = Arrays.stream(sizes, 0, 2).sum();
        var longCount = Arrays.stream(sizes, 2, sizes.length).sum();
        var totalCount = Arrays.stream(sizes).sum();
        System.out.printf("Short: %.2f%%, long: %.2f%%%n",
                100.0 * shortCount / totalCount, 100.0 * longCount / totalCount);
        for (int i = 0; i < sizes.length; i++) {
//            System.out.printf("%2d -> %,d%n", 4 * i, sizes[i]);
            System.out.printf("%2d..%2d -> %s%n", i * 4 + 1, (i + 1) * 4, "X".repeat(sizes[i] / scale));
        }
    }
    public static void branchPrediction() throws Exception {
        var in = new BufferedReader(new FileReader(MEASUREMENTS_TXT));
        var hitCount = 0;
        var missCount = 0;
        var takenCount = 0;
        var notTakenCount = 0;
        var lineCount = 0;
        var saturatingCounter = 0;
        while (true) {
            var line = in.readLine();
            if (line == null) {
                break;
            }
            if (++lineCount % 10_000_000 == 0) {
                System.out.printf("%,d%n", lineCount);
            }
            var nameLen = line.indexOf(';') + 1;
            var branchTaken = nameLen > 8;
            if (branchTaken) {
                takenCount++;
            } else {
                notTakenCount++;
            }
            var prediction = saturatingCounter >= 2;
//            System.out.printf("nameLen %2d, %5s, %5s\n", nameLen, branchTaken, prediction == branchTaken);
            var predictionCorrect = branchTaken == prediction;
            if (predictionCorrect) {
                hitCount++;
            } else {
                missCount++;
            }
            if (branchTaken) {
                saturatingCounter = Math.min(3, saturatingCounter + 1);
            } else {
                saturatingCounter = Math.max(0, saturatingCounter - 1);
            }
        }
        System.out.println(hitCount + missCount == lineCount);
        System.out.printf("Taken: %,d (%.1f%%), not taken: %,d (%.1f%%), hits: %,d (%.1f%%), misses: %,d (%.1f%%).\n",
                takenCount, 100.0 * takenCount / lineCount, notTakenCount, 100.0 * notTakenCount / lineCount,
                hitCount, 100.0 * hitCount / lineCount, missCount, 100.0 * missCount / lineCount);
    }
}

// Byte distributoion in Official dataset:
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

// Byte distribution in 10k dataset:
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
