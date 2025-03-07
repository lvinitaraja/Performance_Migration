package org.migration.db.client;

import com.collabrr.db.connection.ex.ConnectionException;
import com.collabrr.storeentity.db.config.DBConnectionConfig;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.vg.pw.xmlprocessor.Element;
import com.vg.pw.xmlprocessor.Node;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.migration.script.MigrationBL.logMessage;

public class GCPFirestoreDBConnectionManager {

    private Firestore database;

    /**
     *
     * <db-connection type="gcp_firestore" >
     <connection>
     <gcp-firestore>
     <database-url>https://micras.firebaseio.com</database-url>
     </gcp-firestore>
     </connection>
     </db-connection>

     * @param connectionConfig
     * @throws ConnectionException
     * @throws Exception
     */
    private GCPFirestoreDBConnectionManager(String databaseURL, String projectName, String databaseName)  throws IOException, ConnectionException{
        logMessage("*GCPFirestoreDBConnectionManager");
        if (isNotBlank(databaseURL) && isNotBlank(databaseName)) {

            FirebaseOptions.Builder builder = FirebaseOptions.builder();
            FirebaseOptions options = builder
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .setDatabaseUrl(databaseURL)
                    .setProjectId(projectName)
                    .build();

            FirebaseApp fbApp =FirebaseApp.initializeApp(options,databaseName);

            this.database = FirestoreClient.getFirestore(fbApp, databaseName);

        } else {
            throw new ConnectionException("###Firebase Database Name is not configured. So not able to start the processor");
        }

        if(this.database == null) {
            throw new ConnectionException("###Firebase Database connection is not established. Please check the store connection configuration and check database is running or not.");
        }
    }

    private static GCPFirestoreDBConnectionManager connectionManager = null;

    public static GCPFirestoreDBConnectionManager getInstance(String databaseURL, String projectName, String databaseName) throws ConnectionException{
        logMessage("*getInstance");

        if(connectionManager == null) {
            try {
                connectionManager = new GCPFirestoreDBConnectionManager(databaseURL, projectName, databaseName);
            } catch(Exception e) {
                logMessage("###Error occured while creating Firestore Configuration " + ExceptionUtils.getStackTrace(e));
                throw new ConnectionException("###Error occured while creating Firestore Configuration ", e);

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
