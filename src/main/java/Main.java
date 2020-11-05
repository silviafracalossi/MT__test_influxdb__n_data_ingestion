import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.*;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

  // Store users' configurations - default settings written here
  static Scanner sc = new Scanner(System.in);
  static boolean useServerInfluxDB = true;
  static String data_file_path = "data/TEMPERATURE_nodup.csv";
  static int N = 0;

  // Logger names date formatter
  static String logs_path = "logs/";
  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
    "YYYY-MM-dd__HH.mm.ss");

  // Creating the database interactor
  static DatabaseInteractions dbi;


	public static void main(String[] args) throws IOException {

    try {

      // Getting information from user
      if (args.length != 3) {
        talkToUser();
      } else {

        // Getting the max size of tuples inserted together
        N = Integer.parseInt(args[0]);

        // Understanding the DB required
        if (args[1].compareTo("l") == 0) {
          useServerInfluxDB = false;
        }

        // Understanding the data file name
        File f = new File("data/"+args[2]);
        if(f.exists() && !f.isDirectory()) {
          data_file_path = "data/"+args[2];
        }
      }

      // Loading the credentials to the new influxdb database
      System.out.println("Instantiating database interactor");
      dbi = new DatabaseInteractions(data_file_path, useServerInfluxDB);

      // Marking start of tests
      System.out.println("---Start of Tests!---");

      // Repeating the test N times
      for (int i=1; i<=N; i++) {

        // Creating logger
        Logger logger = instantiateLogger(i);
        logger.info("Test inserting \"" +i+"\" tuples at a time");

        // Opening a connection to the influxdb database
        logger.info("Connecting to the InfluxDB database...");
        dbi.createDBConnection();
        dbi.createDatabase();

        // ==START OF TEST==
        System.out.println(i);
        dbi.insertNTuples(i, logger);

        // ==END OF TEST==
        logger.info("--End of test #"+i+"--");

        // Clean database and close connections
        endOfTest();

      }
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        dbi.closeDBConnection();
    }
  }

  //-----------------------UTILITY----------------------------------------------

  // Interactions with the user to understand his/her preferences
  public static void talkToUser () throws Exception {

    System.out.println("3 questions for you!");

    String response = "";
    boolean correct_answer = false;

    // Understanding the N
    while (N < 1) {
      System.out.print("1. What is the max number of tuples inserted together?: ");
      N = Integer.parseInt(sc.nextLine());
    }

    // Understanding whether the user wants the sever db or the local db
    response = "";
    correct_answer = false;
    while (!correct_answer) {
      System.out.print("2. Where do you want it to be executed?"
      +" (Type \"s\" for server database,"
      +" type \"l\" for local database): ");
      response = sc.nextLine().replace(" ", "");

      // Understanding what the user wants
      if (response.compareTo("l") == 0 || response.compareTo("s") == 0) {
        correct_answer=true;
        if (response.compareTo("l") == 0) {
          useServerInfluxDB = false;
        }
      }
    }

    // Understanding which file to run
    response = "";
    correct_answer = false;
    while (!correct_answer) {
      System.out.print("3. Finally, inside the data folder, what is the name" +
      " of the file containing the data to be inserted? ");
      response = sc.nextLine().replace(" ", "");

      // Checking if it is a file
      File f = new File("data/"+response);
      if(f.exists() && !f.isDirectory()) {
        data_file_path = "data/"+response;
        correct_answer = true;
      }
    }

    System.out.println("We are ready to start, thank you!");
  }

  // Instantiating the logger for the general information or errors
  public static Logger instantiateLogger (int i) throws IOException {

    // Retrieving and formatting current timestamp
    Date date = new Date();
    Timestamp now = new Timestamp(date.getTime());
    String dateAsString = simpleDateFormat.format(now);

    // Setting the name of the folder
    if (i == 1) {
      logs_path += dateAsString+"__"+N+"/";
      File file = new File(logs_path);
      boolean bool = file.mkdirs();
    }

    // Instantiating general logger
    String log_complete_path = logs_path + dateAsString + "__" + i
        + "__N_influxdb_data_ingestion.xml";
    Logger logger = Logger.getLogger("NDataIngestion_"+i);
    logger.setLevel(Level.ALL);

    // Loading properties of log file
    Properties preferences = new Properties();
    try {
        FileInputStream configFile = new FileInputStream("resources/logging.properties");
        preferences.load(configFile);
        LogManager.getLogManager().readConfiguration(configFile);
    } catch (IOException ex) {
        System.out.println("[WARN] Could not load configuration file");
    }

    // Instantiating file handler
    FileHandler gl_fh = new FileHandler(log_complete_path);
    logger.addHandler(gl_fh);

    // Returning the logger
    return logger;
  }


  // Cleans the database and closes all the connections to it
  public static void endOfTest() {
      dbi.closeDBConnection();
  }
}
