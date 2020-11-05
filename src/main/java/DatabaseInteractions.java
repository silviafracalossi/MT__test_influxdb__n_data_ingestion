import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.*;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;

import java.text.*;

public class DatabaseInteractions {

  // DB variables
  static InfluxDB influxDB = null;
  static boolean useServerInfluxDB = false;

  // Databases URLs
  static final String serverURL = "http://ironmaiden.inf.unibz.it:8086";
  static final String localURL = "http://localhost:8086";

  // Databases Username, Password and Database name
  static final String username = "root";
  static final String password = "root";
  static final String dbName = "test_table";

  // Retention policy definition
  static String retention_policy_name = "testPolicy";
  static String duration = "INF";
  static String replication = "1";

  // Logger formatter
  static SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

  // Location of file containing data
  String data_file_path;

  // Constructor
  public DatabaseInteractions(String data_file_path, Boolean useServerInfluxDB) {
    this.useServerInfluxDB=useServerInfluxDB;
    this.data_file_path=data_file_path;
  }
  
  // Method called from for-loop in main, choosing correct method for insertion
  public void insertNTuples (int N, Logger logger) {
    if (N==0)   return;
    if (N==1)   insertOneTuple(logger);
    else        insertMultipleTuples(N, logger);
  }

  // Iterating through data, inserting it one at a time
  public void insertOneTuple(Logger logger) {

    // Defining variables useful for method
    String[] fields;
    int rows_inserted = 0;

    try {

      // Preparing file scanner
      Scanner reader = new Scanner(new File(data_file_path));

      // Signaling start of test
      logger.info("--Start of test--");
      while (reader.hasNextLine()) {

        // Retrieving the data and preparing insertion script
        fields = reader.nextLine().split(",");

        // Creating point and writing it to the DB
        Point point = Point.measurement("temperature")
                .time(Long.parseLong(fields[0]), TimeUnit.NANOSECONDS)
                .addField("value", Integer.parseInt(fields[1]))
                .build();

        // Inserting data + Catching exception in case of database not working
        try {
          influxDB.write(dbName, "testPolicy", point);
          logger.info("Query executed: ("+fields[0]+","+fields[1]+")\n");
          rows_inserted++;
        } catch (InfluxDBException e) {
          System.out.println("Problems with executing the query on the DB");
          logger.severe("Problems with executing query: ("+fields[0]+","+fields[1]+")\n");
        }
      }

      // Closing the file reader
      reader.close();

    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      logger.severe("Insertion: \"1\" - problems with the execution");
    }

    // Checking the number of rows inserted
    int rows_count = getRowsInDatabase();
    if (rows_count == rows_inserted) {
      logger.info("Total rows inserted: "+rows_inserted);
    } else {
      logger.severe("Supposed rows inserted: "+rows_inserted+" but found "+rows_count);
    }
  }


  // Iterating through data, inserting it i at a time
  public void insertMultipleTuples(int N, Logger logger) {

    // Defining variables useful for method
    String[] fields;
    int rows_inserted = 0;

    // Number of tuples inserted in the batch but not yet executed
    int no_rows_waiting = 0;

    // Storing text for logger
    String logger_text = "";

    try {
      Scanner reader = new Scanner(new File(data_file_path));

      // Signaling start of test
      logger.info("--Start of test--");
      BatchPoints batchPoints = BatchPoints
        .database(dbName)
        .retentionPolicy(retention_policy_name)
        .build();
      Point point;

      while (reader.hasNextLine()) {

        // Retrieving the data and preparing insertion script
        fields = reader.nextLine().split(",");

        // Creating point and writing the logger info in variable
        point = Point.measurement("temperature")
                .time(Long.parseLong(fields[0]), TimeUnit.NANOSECONDS)
                .addField("value", Integer.parseInt(fields[1]))
                .build();
        batchPoints.point(point);
        logger_text += "(\"+fields[0]+\",\"+fields[1]+\") ";
        no_rows_waiting++;

        // Executing the query and checking the result, if number of rows is enough
        if (no_rows_waiting == N) {

          // Inserting data + Catching exception in case of database not working
          try {
            influxDB.write(batchPoints);
            logger.info("Query executed on "+logger_text);
            rows_inserted += no_rows_waiting;
          } catch (InfluxDBException e) {
            System.out.println("Problems with executing the query on the DB");
            logger.severe("Problems with executing query: "+logger_text+"\n");
          }

          // Resetting variables for successive tuples
          batchPoints = BatchPoints
                  .database(dbName)
                  .retentionPolicy(retention_policy_name)
                  .build();
          logger_text = "";
          no_rows_waiting = 0;
        }
      }

      // In case some tuples need to be inserted
      if (no_rows_waiting != 0) {

        // Inserting data + Catching exception in case of database not working
        try {
          influxDB.write(batchPoints);
          logger.info("Query executed on "+logger_text);
          rows_inserted += no_rows_waiting;
        } catch (InfluxDBException e) {
          System.out.println("Problems with executing the query on the DB");
          logger.severe("Problems with executing query: "+logger_text+"\n");
        }
      }

      // Closing the file reader
      reader.close();

    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      logger.severe("Insertion: \"Multiple tuples at a time\" - problems with the execution");
    }

    // Checking the number of rows inserted
    int rows_count = getRowsInDatabase();
    if (rows_count == rows_inserted) {
      logger.info("Total rows inserted: "+rows_inserted);
    } else {
      logger.severe("Supposed rows inserted: "+rows_inserted+" but found "+rows_count);
    }
  }


  //----------------------DATABASE UTILITY--------------------------------------

  // Connecting to the InfluxDB database
  public static boolean createDBConnection() {
    String pos_complete_url;
    if (useServerInfluxDB) {
      influxDB = InfluxDBFactory.connect(serverURL, username, password);
    } else {
      influxDB = InfluxDBFactory.connect(localURL, username, password);
    }

    // Pinging the DB
    Pong response = influxDB.ping();
    return !(response.getVersion().equalsIgnoreCase("unknown"));
  }

  // Creating the table "test_table" in the database
  public static void createDatabase () {
    removeDatabase();
    influxDB.createDatabase(dbName);

    // CREATE RETENTION POLICY testPolicy ON test_table DURATION INF REPLICATION 1
    String query_string = "CREATE RETENTION POLICY "+retention_policy_name+" ON "+dbName+
            " DURATION "+duration+" REPLICATION "+replication+" DEFAULT";
    influxDB.query(new Query(query_string, dbName));
    influxDB.setRetentionPolicy("testPolicy");
  }

  // Get the number of rows present in the database
  public static int getRowsInDatabase() {
    QueryResult queryResult;
    String count_query = "SELECT COUNT(*) FROM \"temperature\"";
    queryResult = influxDB.query(new Query(count_query, dbName));

    String count_in_string = (queryResult.getResults().get(0).getSeries()
            .get(0).getValues().get(0).get(1)) + "";
    int count = Integer.parseInt((count_in_string).substring(0, count_in_string.length() - 2));
    return (count > -1) ? count : 0;
  }

  // Dropping the table "test_table" from the database
  public static void removeDatabase() {
    try {
      influxDB.deleteDatabase(dbName);
    } catch (NullPointerException e) {
      System.out.println("Test table was already removed");
    }
  }

  // Closing the connections to the database
  public static void closeDBConnection() {
    try {
      influxDB.close();
    } catch (NullPointerException e) {
      System.out.println("Closing DB connection - NullPointerException");
    }
  }
}
