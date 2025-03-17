package org.migration.script;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import org.migration.Main;
import org.migration.db.client.GCPFirestoreDBConnectionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MigrationBL {

    private static final Logger logger = Logger.getLogger(MigrationBL.class.getName());

    private final String databaseUrl;

    private GCPFirestoreDBConnectionManager gcpFirestoreDBConnectionManager = null;

    private final String projectName;

    private final String databaseName;

    private static final String PATH_DELIMITER = "/";

    public MigrationBL(String databaseURL, String projectName, String databaseName) throws Exception {
        this.databaseUrl = databaseURL;
        this.projectName = projectName;
        this.databaseName = databaseName;
        this.gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(databaseUrl, projectName, databaseName);
    }

    public void migrateToNewDBStructure(String collectionName, String entityName, Integer batchSize) {

        logMessage("Entering migrateToNewDBStructure Method ..");

        Firestore firestore = null;
        try {
            logMessage("Establishing connection to Firestore DB ..");
            gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(databaseUrl, projectName, databaseName);
            firestore = gcpFirestoreDBConnectionManager.getDatabase();

            logMessage("Connected to DB");

            CollectionReference collectionRef = firestore.collection(collectionName);

            // Create a query to filter documents where data.etp is in etpList
            Query query = collectionRef.whereEqualTo("data.etp", entityName);

            QuerySnapshot querySnapshot;
            QueryDocumentSnapshot lastDocument = null;
            List<QueryDocumentSnapshot> documents;

            int totalMigrated = 0; // Track total migrated documents

            do {
                // If there's a last document, use startAfter to get the next batch
                if (lastDocument != null) {
                    query = query.startAfter(lastDocument);
                }

                // Limit the query to the batch size
                query = query.limit(batchSize);

                // Execute the query to get the next batch
                querySnapshot = query.get().get();
                documents = querySnapshot.getDocuments();

                int batchMigrated = 0; // Track migrated documents in the current batch

                // Process each document in the batch
                for (QueryDocumentSnapshot document : documents) {
                    logMessage("Processing document: " + document.getId());
                    Map<String, Object> data = document.getData();
                    if (data.containsKey("data") && data.get("data") instanceof Map) {
                        Map<String, Object> nestedData = (Map<String, Object>) data.get("data");

                        Map<String, Object> updatedData = new HashMap<>(data);
                        updatedData.remove("data"); // Remove the old nested data

                        updatedData.putAll(nestedData); // Merge the nested data

                        // Update the document with the flattened data
                        ApiFuture<WriteResult> writeResult = document.getReference().set(updatedData);
                        logMessage("Updated document: " + document.getId() + " at: " + writeResult.get().getUpdateTime());
                        batchMigrated++;
                        totalMigrated++;
                    }
                }

                logMessage("Migrated " + batchMigrated + " documents in this batch.");

                // Get the last document in the batch
                if (!documents.isEmpty()) {
                    lastDocument = documents.get(documents.size() - 1);
                }

            } while (documents.size() == batchSize); // Continue if the batch is full, indicating there might be more documents

            logMessage("Total migrated documents: " + totalMigrated);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (gcpFirestoreDBConnectionManager.getDatabase() != null) {
                gcpFirestoreDBConnectionManager.closeDBConnection();
            }
        }
    }

    private <T> DocumentSnapshot fetchObject(String documentId, String collectionName) throws Exception {

        logMessage("Fetching document with id: " + documentId);
        Firestore firestore = null;
        DocumentSnapshot documentSnapshot;
        try {

            gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(databaseUrl, projectName, databaseName);
            firestore = gcpFirestoreDBConnectionManager.getDatabase();

            logMessage("Connected to DB");

            String documentPath = collectionName + PATH_DELIMITER + documentId;

            logMessage("Fetching document from path: " + documentPath);
            documentSnapshot = firestore.document(documentPath).get().get();

            this.validateNull(documentSnapshot);

        } catch (InterruptedException e) {
            logMessage("Error thrown while fetching ..." + e);
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            logMessage("Error thrown while fetching ..." + e);
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logMessage("Error thrown while fetching ..." + e.getStackTrace());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        logMessage("Fetching Document ....");
        return documentSnapshot;
    }

    public <T> void fetchAndPrint(String documentId, String collectionName) throws Exception {
        logMessage("Fetching Document ....");
        DocumentSnapshot documentSnapshot = fetchObject(documentId, collectionName);

        if (documentSnapshot != null) {

            System.out.println("Document ID: " + documentSnapshot.getId());
            System.out.println("Field 'a': " + documentSnapshot.get("a"));
            System.out.println("Field 'data': " + documentSnapshot.get("data"));

            logMessage("Document fetched successfully");
        }

        if (gcpFirestoreDBConnectionManager.getDatabase() != null) {
            gcpFirestoreDBConnectionManager.closeDBConnection();
        }
    }

    private void validateNull(Object obj) throws Exception {
        if (obj == null) {
            throw new Exception("Object is null");
        }
    }

    public static void logMessage(String message) {
        logger.info(message);
    }

    public <T> void migrateOneRecord(String documentId, String collectionName) {
        try {
            DocumentSnapshot documentSnapshot = fetchObject(documentId, collectionName);
            Map<String, Object> data = documentSnapshot.getData();
            if (data.containsKey("data") && data.get("data") instanceof Map) {
                Map<String, Object> nestedData = (Map<String, Object>) data.get("data");

                Map<String, Object> updatedData = new HashMap<>(data);
                updatedData.remove("data"); // Remove the old nested data

                updatedData.putAll(nestedData);

                // Update the document with the flattened data
                ApiFuture<WriteResult> writeResult = documentSnapshot.getReference().set(updatedData);
                logMessage("Updated document: " + documentSnapshot.getId() + " at: " + writeResult.get().getUpdateTime());
            }
        } catch (Exception ex) {
            logMessage("Error thrown while migrating ..." + ex);
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            if (gcpFirestoreDBConnectionManager.getDatabase() != null) {
                gcpFirestoreDBConnectionManager.closeDBConnection();
            }
        }
    }

    public void migrateToNewDBStructure(String collectionName, List<String> entityNames, Integer batchSize) throws Exception {
        logMessage("Entering migrateToNewDBStructure Method ..");

        Firestore firestore = null;
        try {
            logMessage("Establishing connection to Firestore DB ..");
            gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(databaseUrl, projectName, databaseName);
            firestore = gcpFirestoreDBConnectionManager.getDatabase();

            logMessage("Connected to DB");

            CollectionReference collectionRef = firestore.collection(collectionName);

            // Create a query to filter documents where data.etp is in entityNames list
            Query query = collectionRef.whereIn("data.etp", entityNames);

            QuerySnapshot querySnapshot;
            QueryDocumentSnapshot lastDocument = null;
            List<QueryDocumentSnapshot> documents;

            int totalMigrated = 0; // Track total migrated documents

            do {
                // If there's a last document, use startAfter to get the next batch
                if (lastDocument != null) {
                    query = query.startAfter(lastDocument);
                }

                // Limit the query to the batch size
                query = query.limit(batchSize);

                // Execute the query to get the next batch
                querySnapshot = query.get().get();
                documents = querySnapshot.getDocuments();

                int batchMigrated = 0; // Track migrated documents in the current batch

                // Process each document in the batch
                for (QueryDocumentSnapshot document : documents) {
                    logMessage("Processing document: " + document.getId());
                    Map<String, Object> data = document.getData();
                    if (data.containsKey("data") && data.get("data") instanceof Map) {
                        Map<String, Object> nestedData = (Map<String, Object>) data.get("data");

                        Map<String, Object> updatedData = new HashMap<>(data);
                        updatedData.remove("data"); // Remove the old nested data

                        updatedData.putAll(nestedData); // Merge the nested data

                        // Update the document with the flattened data
                        ApiFuture<WriteResult> writeResult = document.getReference().set(updatedData);
                        logMessage("Updated document: " + document.getId() + " at: " + writeResult.get().getUpdateTime());
                        batchMigrated++;
                        totalMigrated++;
                    }
                }

                logMessage("Migrated " + batchMigrated + " documents in this batch.");

                // Get the last document in the batch
                if (!documents.isEmpty()) {
                    lastDocument = documents.get(documents.size() - 1);
                }

            } while (documents.size() == batchSize); // Continue if the batch is full, indicating there might be more documents

            logMessage("Total migrated documents: " + totalMigrated);

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (gcpFirestoreDBConnectionManager.getDatabase() != null) {
                gcpFirestoreDBConnectionManager.closeDBConnection();
            }
        }
    }

}
