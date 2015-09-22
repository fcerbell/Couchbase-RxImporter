package net.cerbelle.WDI;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import rx.Observer;

/**
 * Created by fcerbell on 16/09/2015.
 * CSVRecord upsert
 */
public class RecordObserver implements Observer<String[]> {
    @Override
    public void onCompleted() {
        System.out.println("Finished.");
    }

    @Override
    public void onError(Throwable exception) {
        System.out.println("Oops!");
        exception.printStackTrace();
    }

    @Override
    public void onNext(String[] r) {
        JsonDocument indicatorsDocument;
        JsonObject indicatorsObject;

//        System.out.println(r[0] + " " + r[1] + " " + r[2] + " " + r[3] + " " + r[4] + " " + r[5] + " (Observed by : " + Thread.currentThread().getName() + ")");
        String Year = r[0];
        String CountryCode = r[1];
        String CountryName = r[2];
        String SerieCode = r[3];
        String SerieName = r[4];
        String Value = r[5];

        indicatorsDocument = Import.WDIBucket.get(Year + "_" + CountryCode);
        if (indicatorsDocument == null) {
            indicatorsObject = JsonObject.empty();
        } else {
            indicatorsObject = indicatorsDocument.content();
        }
        indicatorsObject
                .put("Year", Year)
                .put("CountryCode", CountryCode)
                .put("CountryName", CountryName)
                .put(SerieCode.replace('.', '_'), Double.valueOf(Value));
        if (indicatorsDocument == null) {
            indicatorsDocument = JsonDocument.create(Year + "_" + CountryCode, indicatorsObject);
            Import.WDIBucket.insert(indicatorsDocument);
        } else {
            indicatorsDocument = JsonDocument.create(Year + "_" + CountryCode, indicatorsObject);
            Import.WDIBucket.replace(indicatorsDocument);
        }
    }
}
