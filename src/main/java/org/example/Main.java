package org.example;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.concurrent.TimedSemaphore;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.AnnotatedParameterizedType;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final String URL = "";
    private static final int BATCH_SIZE = 50;
    private static final Gson gson = new Gson();
    private static final String TOKEN = "";
    private static final String CSV_FILE = "/home/kdm1t/Desktop/uuids.csv";
    private static final String TYPE = "ASSIGNMENT_TRANSPORTATION";


    public static void main(String[] args) {
        request();
    }

    private static void request() {
        int counter = 1;

        List<UUID> uuids = readUuidsFromCsv(CSV_FILE);
        List<List<UUID>> batches = partitionList(uuids, BATCH_SIZE);
        TimedSemaphore semaphore = new TimedSemaphore(2, TimeUnit.SECONDS, 1);
        for (List<UUID> batch : batches) {
            try {
                System.out.println("Sending batch: " + counter++);
                semaphore.acquire();
                var requestDto = new RequestDto(batch, TYPE);
                sendApiRequest(requestDto);

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally {
                semaphore.shutdown();
            }
        }
    }

    private static List<UUID> readUuidsFromCsv(String filePath) {
        List<UUID> uuids = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath))) {

            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

            for (CSVRecord record : parser) {
                String uuid = record.get("uuid");
                uuids.add(UUID.fromString(uuid));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return uuids;
    }

    private static List<List<UUID>> partitionList(List<UUID> list, int size) {
        List<List<UUID>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    private static void sendApiRequest(RequestDto requestDto) throws IOException {
        URL url = new URL(URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("X-Auth-Token", TOKEN);
        connection.setRequestProperty("X-User-Locale", "ru_RU");
        connection.setDoOutput(true);

        String json = gson.toJson(requestDto);
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = json.getBytes("utf-8");
            os.write(input, 0, input.length);


            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            if (responseCode != 200) {
                System.out.printf("[%d] Response : %s\nRequest: %s\n", responseCode, responseMessage, json);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
