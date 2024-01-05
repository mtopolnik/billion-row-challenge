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
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

public class Experiments {
    static final long SEMICOLON = 0x3b3b3b3b3b3b3b3bL;
    static final long NEWLINE = 0x0a0a0a0a0a0a0a0aL;
    public static final int PARALLELISM = 8;

    public static final int MAX_NAME_LEN = 100;
    private static final int STATS_TABLE_SIZE = 1 << 15;
    public static final String MEASUREMENTS_TXT = "../1brc/measurements.txt";

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
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[(int) chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(raf, chunkStart, chunkLimit));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
    }

    private static class ChunkProcessor implements Runnable {
        private final long chunkStart;
        private final long chunkLimit;
        private final RandomAccessFile raf;

        private MemorySegment inputMem;
        private MemorySegment hashBuf;
        private StatsAccessor stats;
        private final MemorySegment[] nameBlocks = new MemorySegment[STATS_TABLE_SIZE];

        ChunkProcessor(RandomAccessFile raf, long chunkStart, long chunkLimit) {
            this.raf = raf;
            this.chunkStart = chunkStart;
            this.chunkLimit = chunkLimit;
        }

        @Override public void run() {
            try {
                stats = new StatsAccessor(Arena.ofConfined().allocate(STATS_TABLE_SIZE * StatsAccessor.SIZEOF, Long.BYTES));
                hashBuf = Arena.ofConfined().allocate(16);
                inputMem = raf.getChannel().map(MapMode.READ_ONLY, chunkStart, chunkLimit - chunkStart, Arena.ofConfined());
                long offset = 0;
                while (offset < inputMem.byteSize()) {
                    final long semicolonPos = bytePos(inputMem, offset, (byte) ';');
                    if (semicolonPos == -1) {
                        break;
                    }
                    final long newlinePos = bytePos(inputMem, semicolonPos + 1, (byte) '\n');
                    if (newlinePos == -1) {
                        throw new RuntimeException("No newline after a semicolon!");
                    }
                    recordMeasurement(stats, offset, semicolonPos, newlinePos);
                    offset = newlinePos + 1;
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void recordMeasurement(StatsAccessor stats, long startPos, long semicolonPos, long newlinePos) {
            int temperature = parseTemperature(semicolonPos, newlinePos);
            final long hash = hash(inputMem, startPos, semicolonPos);
            int tableIndex = (int) (hash % STATS_TABLE_SIZE);
            short nameLen = (short) (semicolonPos - startPos);
            MemorySegment nameSlice = inputMem.asSlice(startPos, nameLen);
            while (true) {
                stats.gotoIndex(tableIndex);
                long foundHash = stats.hash();
                if (foundHash == 0) {
                    stats.hash(hash);
                    stats.sum(temperature);
                    stats.count(1);
                    stats.min(temperature);
                    stats.max(temperature);
                    var nameBlock = Arena.ofConfined().allocate(nameLen);
                    nameBlock.copyFrom(nameSlice);
                    nameBlocks[tableIndex] = nameBlock;
                    break;
                }
                if (foundHash != hash || nameBlocks[tableIndex].mismatch(nameSlice) != -1) {
                    tableIndex = (tableIndex + 1) % STATS_TABLE_SIZE;
                    continue;
                }
                stats.sum(stats.sum() + temperature);
                stats.count(stats.count() + 1);
                stats.min(Integer.min(stats.min(), temperature));
                stats.max(Integer.max(stats.max(), temperature));
                break;
            }
        }

        private int parseTemperature(long start, long limit) {
            final byte zeroChar = (byte) '0';
            int temperature = inputMem.get(JAVA_BYTE, limit - 1) - zeroChar;
            temperature += 10 * (inputMem.get(JAVA_BYTE, limit - 3) - zeroChar);
            if (limit - 4 > start) {
                final byte b = inputMem.get(JAVA_BYTE, limit - 4);
                if (b != (byte) '-') {
                    temperature += 100 * (b - zeroChar);
                    if (limit - 5 > start) {
                        temperature = -temperature;
                    }
                } else {
                    temperature = -temperature;
                }
            }
            return temperature;
        }

        private long hash(MemorySegment inputMem, long start, long limit) {
            hashBuf.set(JAVA_LONG, 0, 0);
            hashBuf.set(JAVA_LONG, 8, 0);
            hashBuf.copyFrom(inputMem.asSlice(start, Long.min(16, limit - start)));
            long n1 = hashBuf.get(JAVA_LONG, 0);
            long n2 = hashBuf.get(JAVA_LONG, 8);
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 19;
            long hash = n1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            hash ^= n2;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            return hash != 0 ? hash & (~Long.MIN_VALUE) : 1;
        }
    }

    static long bytePos(MemorySegment haystack, long start, byte needle) {
        return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? bytePosLittleEndian(haystack, start, needle) :
                bytePosBigEndian(haystack, start, needle);
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

    private static long simpleSearch(MemorySegment haystack, byte needle, long offset) {
        for (; offset < haystack.byteSize(); offset++) {
            if (haystack.get(JAVA_BYTE, offset) == needle) {
                return offset;
            }
        }
        return -1;
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

    static class StatsAccessor {
        static final long HASH_OFFSET = 0;
        static final long SUM_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Integer.BYTES;
        static final long SIZEOF = (MAX_OFFSET + Integer.BYTES - 1) / 8 * 8 + 8;

        private final MemorySegment memSeg;
        private long base;

        StatsAccessor(MemorySegment memSeg) {
            this.memSeg = memSeg;
        }

        void gotoIndex(int index) {
            base = index * SIZEOF;
        }

        long hash() {
            return memSeg.get(JAVA_LONG, base + HASH_OFFSET);
        }

        int sum() {
            return memSeg.get(JAVA_INT, base + SUM_OFFSET);
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

        void sum(int sum) {
            memSeg.set(JAVA_INT, base + SUM_OFFSET, sum);
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
}
