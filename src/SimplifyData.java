import com.opencsv.CSVReaderHeaderAware;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

public class SimplifyData {
    // Source: https://simplemaps.com/data/world-cities
    public static void main(String[] args) throws Exception {
        try (var rows = new CSVReaderHeaderAware(new FileReader("data/worldcities.csv"));
             var outFile = new BufferedWriter(new FileWriter("data/weather_stations.csv"))
        ) {
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
