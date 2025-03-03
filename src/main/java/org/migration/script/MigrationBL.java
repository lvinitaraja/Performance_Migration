package org.migration.script;

import com.collabrr.db.connection.ex.ConnectionException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import org.migration.db.client.GCPFirestoreDBConnectionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MigrationBL {

    private static final String DATABASE_URL = "https://console.cloud.google.com/firestore/databases/-default-/data/panel/Accounts/A000000000?inv=1&invt=AbrB7g&project=gp-rnd-composer-dev";

    private static final String DATABASE_NAME = "gp-rnd-composer-dev";

    private static final String collectionName = "DTR";

    private static final List<String> etpList = List.of("View", "EmailTemplate", "ExternalWebService", "UIModule", "WebServiceConnection", "WorkflowDefinition");

    public void migrateToNewDBStructure()
    {

        GCPFirestoreDBConnectionManager gcpFirestoreDBConnectionManager = null;
        Firestore firestore = null;
        try {

            gcpFirestoreDBConnectionManager = GCPFirestoreDBConnectionManager.getInstance(DATABASE_URL, DATABASE_NAME);
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

}
