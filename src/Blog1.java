import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

public class Blog1 {
    public static void main(String[] args) throws Exception {
        var start = System.currentTimeMillis();
var allStats = new BufferedReader(new FileReader("measurements.txt"))
        .lines()
        .parallel()
        .collect(
                groupingBy(line -> line.substring(0, line.indexOf(';')),
                summarizingDouble(line ->
                        parseDouble(line.substring(line.indexOf(';') + 1)))));
var result = allStats.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        e -> {
            var stats = e.getValue();
            return String.format("%.1f/%.1f/%.1f",
                    stats.getMin(), stats.getAverage(), stats.getMax());
        },
        (l, r) -> r,
        TreeMap::new));
System.out.println(result);
        System.err.format("Took %,d ms\n", System.currentTimeMillis() - start);
    }
}
