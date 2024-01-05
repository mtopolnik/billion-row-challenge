import com.opencsv.CSVReaderHeaderAware;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

public class SimplifyData {
    public static void x(String[] args) throws Exception {
        try (var rows = new CSVReaderHeaderAware(new FileReader("data/weather_stations.csv"))) {
            while (true) {
                Map<String, String> row = rows.readMap();
                if (row == null) {
                    break;
                }
                var city = row.get("city");
                var lat = row.get("lat");
                if (city.indexOf(';') != -1) {
                    System.out.println("problem: " + city);
                    System.exit(0);
                }
                System.out.format("%s: %s%n", city, lat);
            }
        }
    }

    // Source: https://simplemaps.com/data/world-cities
    public static void main(String[] args) throws Exception {
        try (var rows = new CSVReaderHeaderAware(new FileReader("data/worldcities.csv"));
             var outFile = new BufferedWriter(new FileWriter("data/weather_stations.csv"))
        ) {
//            outFile.write("\"city\",\"lat\"\n");
            while (true) {
                Map<String, String> row = rows.readMap();
                if (row == null) {
                    break;
                }
                var city = row.get("city");
                var lat = row.get("lat");
                outFile.write(String.format("%s;%s%n", city, lat));
            }
        }
    }
}
