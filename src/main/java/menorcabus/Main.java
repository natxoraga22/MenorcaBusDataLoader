package menorcabus;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;


public class Main {

    public static void main(String[] args) {
        try {
            // INIT FIRESTORE
            InputStream serviceAccount = Main.class.getResourceAsStream("/menorcabus-firebase-adminsdk-w7y9z-d0e3eb72cd.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
            FirestoreOptions options = FirestoreOptions.newBuilder().setTimestampsInSnapshotsEnabled(true).setCredentials(credentials).build();
            Firestore db = options.getService();

            // INIT CSV
            String csvPath = "/Users/natxoraga/Downloads/cime-menorca-es/";
            List<String> collectionNames = Arrays.asList("calendar", "routes", "stop_times", "stops", "trips");
            List<String> collectionDocumentKeyColumns = Arrays.asList("service_id", "route_id", "trip_id,stop_sequence", "stop_id", "trip_id");

            for (int i = 0; i < collectionNames.size(); i++) {
                String collectionName = collectionNames.get(i);
                String collectionDocumentKeyColumn = collectionDocumentKeyColumns.get(i);

                CollectionReference collRef = db.collection(collectionName);
                deleteCollection(collRef);

                Reader in = new FileReader(csvPath + collectionName + ".txt");
                Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);

                for (CSVRecord record : records) {
                    //System.out.println(record.toMap());
                    String[] collectionDocumentKeyColumnSplit = collectionDocumentKeyColumn.split(",");
                    StringJoiner stringJoiner = new StringJoiner("__");
                    for (String collectionDocumentKeyMember : collectionDocumentKeyColumnSplit) {
                        stringJoiner.add(record.get(collectionDocumentKeyMember));
                    }

                    ApiFuture<WriteResult> result = collRef.document(stringJoiner.toString()).set(record.toMap());
                    System.out.println("Update time : " + result.get().getUpdateTime());
                }
            }


            /*
            // asynchronously retrieve all users
            ApiFuture<QuerySnapshot> query = db.collection("routes").get();
            // ...
            // query.get() blocks on response
            QuerySnapshot querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
            for (QueryDocumentSnapshot document : documents) {
                System.out.println(document.getData());
            }
            */
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteCollection(CollectionReference collection) {
        try {
            ApiFuture<QuerySnapshot> future = collection.get();
            List<QueryDocumentSnapshot> documents = future.get().getDocuments();
            for (QueryDocumentSnapshot document : documents) {
                document.getReference().delete();
            }
        }
        catch (Exception e) {
            System.err.println("Error deleting collection : " + e.getMessage());
        }
    }

}
