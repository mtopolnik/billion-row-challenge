import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class Experiments {
    static final long SEMICOLON = 0x3b3b3b3b3b3b3b3bL;
    static final long NEWLINE = 0x0a0a0a0a0a0a0a0aL;
    public static final int PARALLELISM = 8;

    public static final int MAX_NAME_LEN = 100;
    public static final int KEYSET_SIZE = 10_000;
    public static final String MEASUREMENTS_TXT = "../1brc/measurements.txt";
    private static final int TABLE_SIZE = 1 << 15;

    static class StatsAccessor {
        static final long HASH_OFFSET = 0;
        static final long SUM_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Long.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Integer.BYTES;
        static final long SIZEOF = (MAX_OFFSET + Integer.BYTES - 1) / 8 * 8 + 8;

        private final MemorySegment memSeg;
        private long base;

        StatsAccessor(MemorySegment memSeg) {
            this.memSeg = memSeg;
        }

        void gotoIndex(long index) {
            base = index * SIZEOF;
        }

        long hash() {
            return memSeg.get(JAVA_LONG, base + HASH_OFFSET);
        }
        long sum() {
            return memSeg.get(JAVA_LONG, base + SUM_OFFSET);
        }
        int count() {
            return memSeg.get(JAVA_INT, base + COUNT_OFFSET);
        }
        int min() {
            return memSeg.get(JAVA_INT, base + MIN_OFFSET);
        }
        int max() {
            return memSeg.get(JAVA_INT, base + MAX_OFFSET);
        }

        void hash(long hash) {
            memSeg.set(JAVA_LONG, base + HASH_OFFSET, hash);
        }
        void sum(long sum) {
            memSeg.set(JAVA_LONG, base + SUM_OFFSET, sum);
        }
        void count(int count) {
            memSeg.set(JAVA_INT, base + COUNT_OFFSET, count);
        }
        void min(int min) {
            memSeg.set(JAVA_INT, base + MIN_OFFSET, min);
        }
        void max(int max) {
            memSeg.set(JAVA_INT, base + MAX_OFFSET, max);
        }
    }

    static class StationStats {
        int count;
        double sum;
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;

        @Override public String toString() {
            return String.format("%.1f/%.1f/%.1f", min, sum / count, max);
        }
    }

    public static void main(String[] args) throws Exception {
        var start = System.currentTimeMillis();
        calculate();
        System.out.printf("Took %,d ms%n", System.currentTimeMillis() - start);
    }

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final var results = new StationStats[(int) chunkCount];
        final var chunkStartOffsets = new long[chunkCount];
        Arrays.setAll(results, i -> new StationStats());
        try (var raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n');
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[(int) chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(() -> {
                    try {
                        long chunkSize = chunkLimit - chunkStart;
                        final var inputMem = raf.getChannel().map(MapMode.READ_ONLY, chunkStart, chunkSize, Arena.ofConfined());
                        final var buf = Arena.ofConfined().allocate(16);
                        final var statsMem = Arena.ofConfined().allocate(TABLE_SIZE * StatsAccessor.SIZEOF, Long.BYTES);
                        final var stats = new StatsAccessor(statsMem);
                        long offset = 0;
                        long numEntries = 0;
                        while (offset < chunkSize) {
                            final long semicolonPos = bytePos(inputMem, offset, (byte) ';');
                            if (semicolonPos == -1) {
                                break;
                            }
                            final long hash = hash(inputMem, offset, semicolonPos, buf);
                            final long newlinePos = bytePos(inputMem, semicolonPos + 1, (byte) '\n');
                            if (newlinePos == -1) {
                                throw new RuntimeException("No newline after a semicolon!");
                            }
                            int temperature = parseTemperature(inputMem, newlinePos, semicolonPos);
                            long tableIndex = hash % TABLE_SIZE;
                            var collisionCount = 0;
                            while (true) {
                                stats.gotoIndex(tableIndex);
                                long foundHash = stats.hash();
                                if (foundHash == 0) {
                                    stats.hash(hash);
                                    stats.sum(temperature);
                                    stats.count(1);
                                    stats.min(temperature);
                                    stats.max(temperature);
                                    numEntries++;
                                    if (numEntries == TABLE_SIZE) {
                                        throw new RuntimeException("Stats table is full!");
                                    }
                                    break;
                                } else if (foundHash != hash) {
                                    tableIndex = (tableIndex + 1) % TABLE_SIZE;
                                    collisionCount++;
                                    continue;
                                }
                                stats.sum(stats.sum() + temperature);
                                stats.count(stats.count() + 1);
                                stats.min(Integer.min(stats.min(), temperature));
                                stats.max(Integer.max(stats.max(), temperature));
                                break;
                            }
                            offset = newlinePos + 1;
                        }
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
    }

    private static int parseTemperature(MemorySegment inputMem, long newlinePos, long semicolonPos) {
        final byte zeroChar = (byte) '0';
        int temperature = inputMem.get(JAVA_BYTE, newlinePos - 1) - zeroChar;
        temperature += 10 * (inputMem.get(JAVA_BYTE, newlinePos - 3) - zeroChar);
        if (newlinePos - 4 > semicolonPos) {
            final byte b = inputMem.get(JAVA_BYTE, newlinePos - 4);
            if (b != (byte) '-') {
                temperature += 100 * (b - zeroChar);
                if (newlinePos - 5 > semicolonPos) {
                    temperature = -temperature;
                }
            } else {
                temperature = -temperature;
            }
        }
        return temperature;
    }

    private static long hash(MemorySegment inputMem, long offset, long limit, MemorySegment buf) {
        buf.set(JAVA_LONG, 0, 0);
        buf.set(JAVA_LONG, 8, 0);
        buf.copyFrom(inputMem.asSlice(offset, Long.min(16, limit - offset)));
        long n1 = buf.get(JAVA_LONG, 0);
        long n2 = buf.get(JAVA_LONG, 8);
        long seed = 0x51_7c_c1_b7_27_22_0a_95L;
        int rotDist = 8;
        long hash = n1;
        hash *= seed;
        hash = Long.rotateLeft(hash, rotDist);
        hash ^= n2;
        hash *= seed;
        hash = Long.rotateLeft(hash, rotDist);
        return hash != 0 ? hash & (~Long.MIN_VALUE) : 1;
    }

    static long bytePos(MemorySegment haystack, long start, byte needle) {
        return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
                ? bytePosLittleEndian(haystack, start, needle)
                : bytePosBigEndian(haystack, start, needle);
    }

    // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
    // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
    static long bytePosLittleEndian(MemorySegment haystack, long start, byte needle) {
        long nnnnnnnn = broadcastByte(needle);
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ nnnnnnnn;
            long matchIndicators = (diff - 0x0101010101010101L) & ~diff & 0x8080808080808080L;
            if (matchIndicators != 0) {
                return offset + Long.numberOfTrailingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, needle, offset);
    }

    // Adapted from https://richardstartin.github.io/posts/finding-bytes
    static long bytePosBigEndian(MemorySegment haystack, long start, byte needle) {
        long nnnnnnnn = broadcastByte(needle);
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ nnnnnnnn;
            long matchIndicators = (diff & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
            matchIndicators = ~(matchIndicators | diff | 0x7F7F7F7F7F7F7F7FL);
            if (matchIndicators != 0) {
                return offset + Long.numberOfLeadingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, needle, offset);
    }

    private static long broadcastByte(byte val) {
        long broadcast = val;
        broadcast |= broadcast << 8;
        broadcast |= broadcast << 16;
        broadcast |= broadcast << 32;
        return broadcast;
    }

    private static void testBytePos() {
        System.out.println(ByteOrder.nativeOrder());
        var memSeg = Arena.ofConfined().allocate(100);
        memSeg.set(JAVA_BYTE, 0, (byte) '\n');
        var posLE = bytePosLittleEndian(memSeg, 0, (byte) '\n');
        var posBE = bytePosBigEndian(memSeg, 0, (byte) '\n');
        System.out.println("Position LE: " + posLE);
        System.out.println("Position BE: " + posBE);
    }

    private static long simpleSearch(MemorySegment haystack, byte needle, long offset) {
        for (; offset < haystack.byteSize(); offset++) {
            if (haystack.get(JAVA_BYTE, offset) == needle) {
                return offset;
            }
        }
        return -1;
    }

    static void calculateBasic() throws Exception {
        var in = new BufferedReader(new FileReader(MEASUREMENTS_TXT));
        var stats = new HashMap<String, StationStats>();
        while (true) {
            var line = in.readLine();
            if (line == null) {
                break;
            }
            var pivot = line.indexOf(';');
            var city = line.substring(0, pivot);
            var temp = Float.parseFloat(line.substring(pivot + 1));
            var cityStats = stats.computeIfAbsent(city, c -> new StationStats());
            cityStats.count++;
            cityStats.sum += temp;
            cityStats.min = Float.min(cityStats.min, temp);
            cityStats.max = Float.max(cityStats.max, temp);
        }
        System.out.println(new TreeMap<>(stats));
    }

    static void hashing() throws Exception {
        var stations = generateWeatherStations();
        var hashes = new HashMap<Long, String>();
        var collisionCount = 0;
        for (var station : stations) {
            byte[] idBytes = Arrays.copyOf(station.name.getBytes(StandardCharsets.UTF_8), 16);
            var buf = ByteBuffer.wrap(idBytes);
            var n1 = buf.getLong(0);
            var n2 = buf.getLong(8);
            var seed = 0x51_7c_c1_b7_27_22_0a_95L;
            var rotDist = 8;
            var id = n1;
            id *= seed;
            id = Long.rotateLeft(id, rotDist);
            id ^= n2;
            id *= seed;
            id = Long.rotateLeft(id, rotDist);
            long hash = id; // & ((1L << 32) - 1);
            String collidingName = hashes.put(hash, station.name);
            if (collidingName != null) {
                System.out.format("Collision! %s and %s, hash is %,d%n", station.name, collidingName, hash);
                collisionCount++;
            }
        }
        System.out.println(collisionCount + " collisions");
    }

    static void lengthDistribution() throws Exception {
        var stations = generateWeatherStations();
        int countUpTo8Bytes = 0;
        int countUpTo16Bytes = 0;
        int countUpTo32Bytes = 0;
        int countUpTo48Bytes = 0;
        int countUpTo64Bytes = 0;
        int countMoreThan64Bytes = 0;
        for (var station : stations) {
            var len = station.name.length();
            if (len <= 8) {
                countUpTo8Bytes++;
            } else if (len <= 16) {
                countUpTo16Bytes ++;
            } else if (len <= 32) {
                countUpTo32Bytes++;
            } else if (len <= 48) {
                countUpTo48Bytes++;
            } else if (len <= 64) {
                countUpTo64Bytes++;
            } else {
                countMoreThan64Bytes++;
            }
        }
        System.out.println("Up to  8 chars: " + countUpTo8Bytes);
        System.out.println("Up to 16 chars: " + countUpTo16Bytes);
        System.out.println("Up to 32 chars: " + countUpTo32Bytes);
        System.out.println("Up to 48 chars: " + countUpTo48Bytes);
        System.out.println("Up to 64 chars: " + countUpTo64Bytes);
        System.out.println("More than 64 chars: " + countMoreThan64Bytes);
    }

    record WeatherStation(String name, float avgTemp) {
    }

    private static ArrayList<WeatherStation> generateWeatherStations() throws Exception {
        // Use a public list of city names and concatenate them all into a long string,
        // which we'll use as a "source of city name randomness"
        var bigName = new StringBuilder(1 << 20);
        // Source: https://simplemaps.com/data/world-cities
        try (var rows = new BufferedReader(new FileReader("data/weather_stations.csv"));) {
            while (true) {
                var row = rows.readLine();
                if (row == null) {
                    break;
                }
                bigName.append(row, 0, row.indexOf(';'));
            }
        }
        final var weatherStations = new ArrayList<WeatherStation>();
        final var names = new HashSet<String>();
        var minLen = Integer.MAX_VALUE;
        var maxLen = Integer.MIN_VALUE;
        try (var rows = new BufferedReader(new FileReader("data/weather_stations.csv"))) {
            final var nameSource = new StringReader(bigName.toString());
            final var buf = new char[MAX_NAME_LEN];
            final var rnd = ThreadLocalRandom.current();
            final double yOffset = 4;
            final double factor = 2500;
            final double xOffset = 0.372;
            final double power = 7;
            for (int i = 0; i < KEYSET_SIZE; i++) {
                var row = rows.readLine();
                if (row == null) {
                    break;
                }
                // Use a 7th-order curve to simulate the name length distribution.
                // It gives us mostly short names, but with large outliers.
                var nameLen = (int) (yOffset + factor * Math.pow(rnd.nextDouble() - xOffset, power));
                minLen = Integer.min(minLen, nameLen);
                maxLen = Integer.max(maxLen, nameLen);
                var count = nameSource.read(buf, 0, nameLen);
                if (count == -1) {
                    throw new Exception("Name source exhausted");
                }
                var name = new String(buf, 0, nameLen).trim();
                while (name.length() < nameLen) {
                    name += readNonSpace(nameSource);
                }
                while (names.contains(name)) {
                    name = name.substring(1) + readNonSpace(nameSource);
                }
                if (name.indexOf(';') != -1) {
                    throw new Exception("Station name contains a semicolon!");
                }
                names.add(name);
                var lat = Float.parseFloat(row.substring(row.indexOf(';') + 1));
                // Guesstimate mean temperature using cosine of latitude
                var avgTemp = (float) (30 * Math.cos(Math.toRadians(lat))) - 10;
                weatherStations.add(new WeatherStation(name, avgTemp));
            }
        }
        System.out.format("Generated %,d station names with length from %,d to %,d%n", KEYSET_SIZE, minLen, maxLen);
        return weatherStations;
    }

    private static char readNonSpace(StringReader nameSource) throws IOException {
        while (true) {
            var n = nameSource.read();
            if (n == -1) {
                throw new IOException("Name source exhausted");
            }
            var ch = (char) n;
            if (ch != ' ') {
                return ch;
            }
        }
    }

    static void calculateChronicle() throws Exception {
        var file = new File("../1brc/measurements.txt");
        long length = file.length();
        var bytes = MappedBytes.singleMappedBytes(file, length, true).readLimit(length);
        var chunkStartOffsets = new long[PARALLELISM];
        chunkStartOffsets[0] = 0;
        for (int i = 1; i < chunkStartOffsets.length; i++) {
            var offset = length * i / chunkStartOffsets.length;
            bytes.readPosition(offset);
            var newlinePos = bytes.findByte((byte) '\n');
            chunkStartOffsets[i] = offset + newlinePos;
        }
        var chunks = new Bytes[chunkStartOffsets.length];
        bytes.readPosition(0);
        bytes.readLimit(chunkStartOffsets[1]);
        chunks[0] = bytes;
        for (int i = 1; i < chunkStartOffsets.length; i++) {
            var chunkBytes = MappedBytes.singleMappedBytes(file, length, true);
            if (i == chunkStartOffsets.length - 1) {
                chunkBytes.readLimit(length);
            } else {
                chunkBytes.readLimit(chunkStartOffsets[i]);
            }
            chunkBytes.readPosition(chunkStartOffsets[i]);
            chunks[i] = chunkBytes;
        }
        var executor = Executors.newFixedThreadPool(8);
        var futures = new ArrayList<Future<Map<String, StationStats>>>();
        for (var chunk : chunks) {
            futures.add(executor.submit(() -> {
                System.out.println("start chunk");
                var stats = new HashMap<String, StationStats>();
                while (!chunk.isEmpty()) {
                    var name = chunk.parseUtf8(ch -> ch == ';' || ch < ' ');
                    var temp = chunk.parseFloat();
                    var cityStats = stats.computeIfAbsent(name, n -> new StationStats());
                    cityStats.count++;
                    cityStats.sum += temp;
                    cityStats.min = Float.min(cityStats.min, temp);
                    cityStats.max = Float.max(cityStats.max, temp);
                }
                return stats;
            }));
        }
        var solutions = new ArrayList<Map<String, StationStats>>();
        for (var future : futures) {
            solutions.add(future.get());
        }
        var total = solutions.get(0);
        for (int i = 1; i < solutions.size(); i++) {
            var solution = solutions.get(i);
            for (var e : solution.entrySet()) {
                var solutionStats = e.getValue();
                var totalStats = total.computeIfAbsent(e.getKey(), n -> new StationStats());
                totalStats.count += solutionStats.count;
                totalStats.sum += solutionStats.sum;
                totalStats.min = Float.min(totalStats.min, solutionStats.min);
                totalStats.max = Float.min(totalStats.max, solutionStats.max);
            }
        }
        System.out.println(new TreeMap<>(total));
        executor.shutdownNow();
    }
}
