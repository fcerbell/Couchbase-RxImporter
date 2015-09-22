package net.cerbelle.WDI;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.CountDownLatch;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by fcerbell on 11/09/2015.
 *
 */
public class Import {

    static String bucketName = "WDI_test";
    static String clusterAddress = "192.168.56.101";
    static String clusterUsername = "Administrator";
    static String clusterPassword = "Administrator";
    static Bucket WDIBucket;

    public static void main(String[] args) throws IOException {
        
        System.out.println("WorldDevelopmentIndicators loader");
        System.out.println("File to load : " + args[0]);

        Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.WARNING);
        for(Handler h : logger.getParent().getHandlers()) {
            if(h instanceof ConsoleHandler){
                h.setLevel(Level.WARNING);
            }
        }

        // Connect to the cluster
        Cluster cluster;
        System.out.println("Cluster connection");
        cluster = CouchbaseCluster.create(clusterAddress);

        // Create a cluster manager
        ClusterManager clusterManager = cluster.clusterManager(clusterUsername,clusterPassword);

        // Drop the bucket if already existing
        if (clusterManager.hasBucket(bucketName)) {
            System.out.println("Drop bucket");
            clusterManager.removeBucket(bucketName);
        }

        // Create the bucket if not already existing
        if (!clusterManager.hasBucket(bucketName)) {
            System.out.println("Create bucket bucket");
            BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
                    .type(BucketType.COUCHBASE)
                    .name(bucketName)
                    .password("")
                    .quota(300) // megabytes
                    .replicas(0)
                    .indexReplicas(false)
                    .enableFlush(false)
                    .build();
            clusterManager.insertBucket(bucketSettings);
        }

        // Open the WDI bucket
        System.out.println("Open bucket");
        WDIBucket = cluster.openBucket(bucketName);

        Reader in = new FileReader(args[0]);
        Iterable<CSVRecord> records = CSVFormat
                .EXCEL
                .withHeader("CountryName", "CountryCode", "SerieName", "SerieCode"
                        , "1960", "1961", "1962", "1963", "1964", "1965", "1966", "1967", "1968", "1969"
                        , "1970", "1971", "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979"
                        , "1980", "1981", "1982", "1983", "1984", "1985", "1986", "1987", "1988", "1989"
                        , "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999"
                        , "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"
                        , "2010", "2011", "2012", "2013", "2014"
                )
                .withSkipHeaderRecord()
                .withNullString("..")
                .withIgnoreEmptyLines()
                .parse(in);

        final CountDownLatch latch = new CountDownLatch(1);
        Observable
                .from(records)
                .filter(r -> !r.get("CountryCode").isEmpty())
                .flatMap(
                        r -> Observable.from(new String[][]{
                                {"1960", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1960")},
                                {"1961", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1961")},
                                {"1962", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1962")},
                                {"1963", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1963")},
                                {"1964", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1964")},
                                {"1965", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1965")},
                                {"1966", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1966")},
                                {"1967", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1967")},
                                {"1968", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1968")},
                                {"1969", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1969")},
                                {"1970", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1970")},
                                {"1971", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1971")},
                                {"1972", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1972")},
                                {"1973", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1973")},
                                {"1974", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1974")},
                                {"1975", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1975")},
                                {"1976", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1976")},
                                {"1977", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1977")},
                                {"1978", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1978")},
                                {"1979", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1979")},
                                {"1980", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1980")},
                                {"1981", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1981")},
                                {"1982", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1982")},
                                {"1983", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1983")},
                                {"1984", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1984")},
                                {"1985", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1985")},
                                {"1986", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1986")},
                                {"1987", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1987")},
                                {"1988", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1988")},
                                {"1989", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1989")},
                                {"1990", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1990")},
                                {"1991", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1991")},
                                {"1992", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1992")},
                                {"1993", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1993")},
                                {"1994", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1994")},
                                {"1995", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1995")},
                                {"1996", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1996")},
                                {"1997", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1997")},
                                {"1998", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1998")},
                                {"1999", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("1999")},
                                {"2000", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2000")},
                                {"2001", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2001")},
                                {"2002", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2002")},
                                {"2003", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2003")},
                                {"2004", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2004")},
                                {"2005", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2005")},
                                {"2006", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2006")},
                                {"2007", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2007")},
                                {"2008", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2008")},
                                {"2009", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2009")},
                                {"2010", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2010")},
                                {"2011", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2011")},
                                {"2012", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2012")},
                                {"2013", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2013")},
                                {"2014", r.get("CountryCode"), r.get("CountryName"), r.get("SerieCode"), r.get("SerieName"), r.get("2014")}
                        })
                )
                .filter(valueLine -> valueLine[5] != null)
                .doOnCompleted(latch::countDown)
                .subscribeOn(Schedulers.computation())
                .subscribe(new RecordObserver());

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


// Disconnect and clear all allocated resources
        cluster.disconnect();
    }
}
