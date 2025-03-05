package org.migration.script;

import com.collabrr.db.connection.ex.ConnectionException;
import com.collabrr.gcp.firestore.db.constant.LoggerConstants;
import com.collabrr.storeentity.db.exception.PlatformEntityException;
import com.collabrr.storeentity.db.exception.PlatformEntityNotFoundException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.migration.db.client.GCPFirestoreDBConnectionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MigrationBL {

//    private static final Logger LOGGER = LogManager.getLogger(MigrationBL.class);

    private final String databaseUrl;

    private final String projectName;

    private final String databaseName;

    private static final String PATH_DELIMITER = "/";

    private static final String collectionName = "DTR";

    private static final List<String> etpList = List.of("View", "EmailTemplate", "ExternalWebService", "UIModule", "WebServiceConnection", "WorkflowDefinition");

    public MigrationBL(String databaseURL, String projectName, String databaseName) {
        this.databaseUrl = databaseURL;
        this.projectName = projectName;
        this.databaseName = databaseName;
    }

    public void migrateToNewDBStructure()
    {

        GCPFirestoreDBConnectionManager gcpFirestoreDBConnectionManager = null;
        Firestore firestore = null;
        try {

            gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(databaseUrl,projectName, databaseName);
            firestore = gcpFirestoreDBConnectionManager.getDatabase();

            CollectionReference collectionRef = firestore.collection(collectionName);

            // Create a query to filter documents where data.etp is in etpList
            Query query = collectionRef.whereIn("data.etp", etpList);

            QuerySnapshot querySnapshot = query.get().get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

            for (QueryDocumentSnapshot document : documents) {
                Map<String, Object> data = document.getData();
                if (data.containsKey("data") && data.get("data") instanceof Map) {
                    Map<String, Object> nestedData = (Map<String, Object>) data.get("data");

                    Map<String, Object> updatedData = new HashMap<>(data);
                    updatedData.remove("data"); // Remove the old nested data

                    updatedData.putAll(nestedData);

                    // Update the document with the flattened data
                    ApiFuture<WriteResult> writeResult = document.getReference().set(updatedData);
                    System.out.println("Updated document: " + document.getId() + " at: " + writeResult.get().getUpdateTime());
                }
            }

        } catch (ConnectionException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if(firestore != null)
            {
                gcpFirestoreDBConnectionManager.closeDBConnection();
            }
        }
    }

    public  <T> void  fetchAndPrint(String documentId, String collectionName, Class<T> returnType) throws PlatformEntityException {

        logMessage("Fetching document with id: " + documentId);
        GCPFirestoreDBConnectionManager gcpFirestoreDBConnectionManager;
        Firestore firestore;
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
            throw new PlatformEntityException(e);
        } catch (ExecutionException e) {
            logMessage("Error thrown while fetching ..." + e);
            e.printStackTrace();
            throw new PlatformEntityException(e);
        } catch (ConnectionException | PlatformEntityNotFoundException e) {
            logMessage("Error thrown while fetching ..." + e.getStackTrace());
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        logMessage("Fetching Document ....");
        T fetchedObj = documentSnapshot.toObject(returnType);

        if(fetchedObj != null) {

            System.out.println("Document ID: " + documentSnapshot.getId());
            System.out.println("Field 'a': " + documentSnapshot.get("a"));
            System.out.println("Field 'data': " + documentSnapshot.get("data"));

            logMessage("Document fetched successfully");
        }
    }

    private void validateNull(Object obj) throws PlatformEntityNotFoundException {
        if (obj == null) {
            throw new PlatformEntityNotFoundException(LoggerConstants.NO_DATA_EXISTS);
        }
    }

    public static void logMessage(String message) {
        System.out.println(message);
    }

}
