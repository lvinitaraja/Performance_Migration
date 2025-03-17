package org.migration;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.migration.script.MigrationBL;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.migration.script.MigrationBL.logMessage;

public class Main {

    public static final int BATCH_SIZE = 50;

    private static String DATABASE_URL;

    private static String PROJECT_NAME;

    private static String DATABASE_NAME;

    private static final String CONFIG_FILE_PATH = "config.properties";

    private static final String JSON_FILE_PATH = "data.json";

    public static void main(String[] args) throws IOException {

        logMessage("Entering Main Method ..");

        JsonObject jsonData = readJSON(JSON_FILE_PATH);
        String collectionName = jsonData.get("collectionName").getAsString();
        List<String> entityList = new Gson().fromJson(jsonData.get("entityList"), List.class);

        logMessage("Collection Name: " + collectionName + " Entity List: " + entityList);

        try {
            loadProperties();
            MigrationBL migrationBL = new MigrationBL(DATABASE_URL, PROJECT_NAME, DATABASE_NAME);
            String documentId = "UIM_1cdv0nm4tjcocnullA0127_AmZJb7MK";
            migrationBL.migrateOneRecord(documentId, collectionName);
            // migrationBL.migrateToNewDBStructure(collectionName, entityList, 50);
//               migrationBL.migrateToNewDBStructure(collectionName, entityList, BATCH_SIZE);
        } catch (Exception e) {
            e.printStackTrace();
            logMessage("Error thrown while fetching ..." + e.getStackTrace());
        }

        logMessage("Exiting Main Method ..");
    }

    private static void loadProperties() {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(CONFIG_FILE_PATH)) {
            properties.load(input);

            DATABASE_URL = properties.getProperty("database.url");
            PROJECT_NAME = properties.getProperty("project.name");
            DATABASE_NAME = properties.getProperty("database.name");

            logMessage("DATABASE_URL: " + DATABASE_URL);
            logMessage("PROJECT_NAME: " + PROJECT_NAME);
            logMessage("DATABASE_NAME: " + DATABASE_NAME);

            if (DATABASE_URL == null || PROJECT_NAME == null || DATABASE_NAME == null) {
                throw new IllegalArgumentException("Missing required properties in config.properties");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Error loading properties file: " + ex.getMessage());
        }
    }

    private static JsonObject readJSON(String filePath) throws IOException {
        try (FileReader reader = new FileReader(filePath)) {
            return JsonParser.parseReader(reader).getAsJsonObject();
        }
    }
}