package org.migration;

import com.collabrr.app.components.vcd.entity.View;
import org.migration.script.MigrationBL;

import java.util.Arrays;

import static org.migration.script.MigrationBL.logMessage;

public class Main {

//    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    private static final String DATABASE_URL = "https://gp-rnd-composer-dev.firebaseio.com";

    private static final String PROJECT_NAME = "gp-rnd-composer-dev";

    private static final String DATABASE_NAME = "(default)";

    public static void main(String[] args) {

        logMessage("Entering Main Method ..");

        String documentId = "FR_103r0fcr0n5hhnullcs22_A4b0aQ0R";
        String collectionName = "DTR";
        Class clazz = View.class;
        try {
            MigrationBL migrationBL = new MigrationBL(DATABASE_URL, PROJECT_NAME, DATABASE_NAME);
            migrationBL.migrateToNewDBStructure(50);
        } catch (Exception e) {
            e.printStackTrace();
            logMessage("Error thrown while fetching ..." + e.getStackTrace());
        }

        logMessage("Exiting Main Method ..");
    }
}