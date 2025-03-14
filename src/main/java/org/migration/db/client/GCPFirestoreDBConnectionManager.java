package org.migration.db.client;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.migration.script.MigrationBL.logMessage;

public class GCPFirestoreDBConnectionManager {

    private Firestore database;

    /**
     * <db-connection type="gcp_firestore" >
     * <connection>
     * <gcp-firestore>
     * <database-url>https://micras.firebaseio.com</database-url>
     * </gcp-firestore>
     * </connection>
     * </db-connection>
     *
     * @param connectionConfig
     * @throws ConnectionException
     * @throws Exception
     */
    private GCPFirestoreDBConnectionManager(String databaseURL, String projectName, String databaseName) throws Exception {
        logMessage("*GCPFirestoreDBConnectionManager");
        if (isNotBlank(databaseURL) && isNotBlank(databaseName)) {

            FirebaseOptions.Builder builder = FirebaseOptions.builder();
            FirebaseOptions options = builder
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .setDatabaseUrl(databaseURL)
                    .setProjectId(projectName)
                    .build();

            FirebaseApp fbApp = FirebaseApp.initializeApp(options, databaseName);

            this.database = FirestoreClient.getFirestore(fbApp, databaseName);

        } else {
            throw new Exception("###Firebase Database Name is not configured. So not able to start the processor");
        }

        if (this.database == null) {
            throw new Exception("###Firebase Database connection is not established. Please check the store connection configuration and check database is running or not.");
        }
    }

    private static GCPFirestoreDBConnectionManager connectionManager = null;

    public static GCPFirestoreDBConnectionManager getInstance(String databaseURL, String projectName, String databaseName) throws Exception {
        logMessage("*getInstance");

        if (connectionManager == null) {
            try {
                connectionManager = new GCPFirestoreDBConnectionManager(databaseURL, projectName, databaseName);
            } catch (Exception e) {
                logMessage("###Error occured while creating Firestore Configuration " + ExceptionUtils.getStackTrace(e));
                throw new Exception("###Error occured while creating Firestore Configuration ", e);

            }
        }
        return connectionManager;
    }


    public Firestore getDatabase() {
        return database;
    }

    /**
     *
     */
    public void closeDBConnection() {
        logMessage("*Shutting down Firestore Connection");
        try {
            this.database.close();
        } catch (Exception e) {
            logMessage("###Exception while closing the Firestore connection.");
        }
    }

}
