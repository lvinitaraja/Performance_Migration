package org.migration;

import org.migration.script.MigrationBL;

import java.util.List;
import java.util.Scanner;

import static org.migration.script.MigrationBL.logMessage;

public class Main {

    private static final String DATABASE_URL = "https://gp-rnd-composer-dev.firebaseio.com";

    private static final String PROJECT_NAME = "gp-rnd-composer-dev";

    private static final String DATABASE_NAME = "(default)";

    public static void main(String[] args) {

        logMessage("Entering Main Method ..");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter Collection Name: ");
        String collectionName = scanner.nextLine();

        System.out.print("Enter Entity Name: ");
        String entityName = scanner.nextLine();
        logMessage("Collection Name: " + collectionName + " Entity Name: " + entityName);

        try {

            MigrationBL migrationBL = new MigrationBL(DATABASE_URL, PROJECT_NAME, DATABASE_NAME);
//            migrationBL.migrateOneRecord(documentId, collectionName);
            migrationBL.migrateToNewDBStructure(collectionName, entityName, 50);
        } catch (Exception e) {
            e.printStackTrace();
            logMessage("Error thrown while fetching ..." + e.getStackTrace());
        }

        logMessage("Exiting Main Method ..");
    }
}