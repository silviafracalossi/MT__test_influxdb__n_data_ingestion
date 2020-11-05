# Test - InfluxDB - Data Ingestion

Tester of the InfluxDB ability of ingesting time series data, from 1 to N tuples at a time

## Repository Structure
-   `data/`, containing the printers parsed logs files in the format of CSV files;
-   `logs/`, containing the log information of all the tests done;
-   `resources/`, containing the InfluxDB driver, the database credentials file and the logger properties;
-   `src/`, containing the java source files;
-   `target/`, containing the generated .class files after compiling the java code.

## Requirements
The repository is a Maven project. Therefore, the dependency that will automatically be downloaded is:
-   InfluxDB JDBC Driver (2.8)

## Installation and running the project
-   Create the folder `data`;
    -   Inside the folder, copy-paste the printers parsed log files, whose timestamp is defined in nanoseconds;
-   Inside the folder `resources`,
    -   Create a file called `server_influxdb_credentials.txt`, containing the username (first line) and the password (second line) to access the server InfluxDB database;
-   Run the project
    -   Open IntelliJ IDEA
    -   Compile the maven project
    -   Execute the main method in the `src/main/java/Main.java` file
    -   If needed, switch the indexes of the databases (see next paragraph).

## Preparing an executable jar file
Since I couldn't manage to find a way with the command line, I used IntelliJ:
-   ---TODO---
-   Execute the JAR file:
    -   If you have this repository available:
        -   From the main directory, execute `java -jar standalone/NDataIngestionTest.jar`.
    -   If you need a proper standalone version:
        -   Check the next paragraph.

## Preparing the standalone version on the server
-   Connect to the unibz VPN through Cisco AnyConnect;
-   Open a terminal:
    -   Execute `ssh -t sfracalossi@ironlady.inf.unibz.it "cd /data/sfracalossi ; bash"`;
    -   Execute `mkdir influxdb/standalone_n_ingestion`;
    -   Execute `mkdir influxdb/standalone_n_ingestion/resources`;
    -   Execute `mkdir influxdb/standalone_n_ingestion/data`;
    -   Execute `mkdir influxdb/standalone_n_ingestion/logs`;
-   Send the JAR and the help files from another terminal (not connected through SSH):
    -   Execute `scp standalone/NDataIngestionTest.jar sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_n_ingestion`;
    -   Execute `scp resources/server_influxdb_credentials.txt sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_n_ingestion/resources`;
    -   Execute `scp resources/logging.properties sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_n_ingestion/resources`;
-   Send the data file:
    -   Execute `scp data/TEMPERATURE_nodup.csv sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_n_ingestion/data`;
-   Execute the JAR file (use the terminal connected through SSH):
    -   Execute `cd standalone_n_ingestion`;
    -   Execute `nohup java -jar NDataIngestionTest.jar [N] [l/s] [file_name_in_data_folder] > logs/out.txt &`.
