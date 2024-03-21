package gov.sandia.idb.database.create;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultDatabaseCreator implements DatabaseCreator {
    final private static Logger logger = LoggerFactory.getLogger(DefaultDatabaseCreator.class);

    private static final String DDL_PATH_STRING = "gov.sandia.idb.db.creation.DDL_PATH_STRING";
    private static final String ENUM_PATH_STRING = "gov.sandia.idb.db.creation.ENUM_PATH_STRING";
    private static final String DB_PROPERTY_FILE_PATH_STRING = "gov.sandia.idb.db.creation.DB_PROPERTY_FILE_PATH_STRING";
    private static final String CREATE_IDB_DATABASE = "gov.sandia.idb.db.creation.CREATE_IDB_DATABASE";
    private static final String DROP_IDB_DATABASE = "gov.sandia.idb.db.creation.DROP_IDB_DATABASE";
    private static final String CONNECTION_STRING_USER_SEPARATOR = "gov.sandia.idb.db.creation.CONNECTION_STRING_USER_SEPARATOR";
    private static final String CONNECTION_STRING_PASSWORD_SEPARATOR = "gov.sandia.idb.db.creation.CONNECTION_STRING_PASSWORD_SEPARATOR";
    private static final String CATALOG_NAME = "gov.sandia.idb.db.creation.CATALOG_NAME";
    private static final String SET_TRANSACTION_ISOLATION_LEVEL = "gov.sandia.idb.db.creation.SET_TRANSACTION_ISOLATION_LEVEL";


    final private String ddlFilePathString;
    final private String enumDMLFilePathString;
    final private String dbConnectorFilePathString;
    final private String databaseCreationString;
    final private String databaseDropString;
    final private String connectionStringUserSeparator;
    final private String connectionStringPasswordSeparator;
    final private String catalogName;
    final private String setTransactionIsolationLevelString;

    public DefaultDatabaseCreator(final String configurationFilePath) {
        logger.info("Using configuration defined in: {}", configurationFilePath);
        final Properties properties = new Properties();

        final URL url = DefaultDatabaseCreator.class
                .getClassLoader().getResource(configurationFilePath);
        if (url == null) {
            throw new RuntimeException("Unable to find configuration file: " + configurationFilePath);
        }
        try {
            properties.load(url.openStream());
        } catch (final Exception e) {
            throw new RuntimeException("Reading config file: " + configurationFilePath, e);
        }

        this.ddlFilePathString = properties.getProperty(DefaultDatabaseCreator.DDL_PATH_STRING);
        this.enumDMLFilePathString = properties.getProperty(DefaultDatabaseCreator.ENUM_PATH_STRING);
        this.dbConnectorFilePathString = properties.getProperty(DefaultDatabaseCreator.DB_PROPERTY_FILE_PATH_STRING);
        this.databaseCreationString = properties.getProperty(DefaultDatabaseCreator.CREATE_IDB_DATABASE);
        this.databaseDropString = properties.getProperty(DefaultDatabaseCreator.DROP_IDB_DATABASE);
        this.connectionStringUserSeparator = properties.getProperty(DefaultDatabaseCreator.CONNECTION_STRING_USER_SEPARATOR);
        this.connectionStringPasswordSeparator = properties.getProperty(DefaultDatabaseCreator.CONNECTION_STRING_PASSWORD_SEPARATOR);
        this.catalogName = properties.getProperty(DefaultDatabaseCreator.CATALOG_NAME);
        this.setTransactionIsolationLevelString = properties.getProperty(DefaultDatabaseCreator.SET_TRANSACTION_ISOLATION_LEVEL);
    }

    @Override
    public void createDatabase(Connection connection) {
        try {
            try (final PreparedStatement statement = connection
                    .prepareStatement(getDatabaseCreationString())) {
                statement.executeUpdate();
            }

            try (final PreparedStatement statement = connection
                    .prepareStatement(this.setTransactionIsolationLevelString)) {
                statement.executeUpdate();

            } catch (final Exception e) {
                throw new RuntimeException("Error creating database: ", e);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Error creating database: ", e);
        }
    }

    /**
     * Read ddl file and create tables in database. Roll-back everything on any
     * error (i.e., table creation is all or nothing). It is assumed that the
     * ddl file has the following format: 1. Ignored lines begin with / or * or
     * empty space; 2. All create tables are terminated by a single semi-colon.
     * 
     * 
     */
    @Override
    public void createTables(final Connection connection, final Path ddlFile) {

        try {

            // ----------------------------------------------------------
            // Read the entire file and then parse each line. Ignore any
            // comment or blank lines and keep building the create statement
            // until a semi-colon is reached which signals the end of
            // a create statement. Then attempt to create the table and, if
            // successful, reset the string builder to start a new create
            // table statement.
            // --------------------------------------------------
            final StringBuilder sb = new StringBuilder();

            for (final String line : Files.readAllLines(ddlFile)) {
                if (isLineEmpty(line)) {
                    continue;
                }
                sb.append(line);
                if (line.trim().endsWith(";") || line.trim().equalsIgnoreCase("GO")) {
                    try (final PreparedStatement statement = connection
                            .prepareStatement(sb.toString().replace(");", ")"))) {
                        logger.debug("Executing string: " + sb.toString());
                        statement.execute();
                        sb.setLength(0);
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Creating tables: " + e.getMessage(), e);
        }
    }

    private boolean isLineEmpty(final String line) {
        if (line == null || line.startsWith("/")
                || line.startsWith("*")
                || line.startsWith(" *")
                || line.startsWith("#")
                || line.isEmpty()) {
            return true;
        }

        return false;
    }

    @Override
    public void loadEnumData(final Connection connection, final Path dataForTypeAndStateFile) {
        try {
        	if (dataForTypeAndStateFile == null || dataForTypeAndStateFile.toString().isEmpty()) {
        		return;
        	}
            // ----------------------------------------------------------
            // Read the entire file and then parse each line. Ignore any
            // comment or blank lines. For each non-comment, non-blank
            // line use the parsed data to:
            // 1. Find the enum corresponding to the type or state table that
            // will be loaded;
            // 2. Use reflection to get the enum's values;
            // 3. Use the remaining parsed data (table name, type or statue
            // ordinal value,
            // type or state description) to create an insert statement for each
            // enum that loads the ordinal and description into the table that
            // describes the enum.
            // --------------------------------------------------
            final StringBuilder sb = new StringBuilder();

            for (final String line : Files.readAllLines(dataForTypeAndStateFile)) {
                if (isLineEmpty(line)) {
                    continue;
                }
                final String[] parsedValues = line.split("::");
                if (parsedValues.length != 5) {
                    throw new RuntimeException("Parsing error while loading enum data, the invalid line is: " + line);
                }

                final String enumValueMethodType = parsedValues[4];
                sb.setLength(0);
                sb.append("INSERT INTO ");
                sb.append(parsedValues[1]);
                sb.append(" VALUES( ?, ? )");
                try (final PreparedStatement statement = connection.prepareStatement(sb.toString())) {
                    final Class<?> enumClass = Class.forName(parsedValues[0]);
                    for (final Enum<?> enumValue : (Enum<?>[]) enumClass.getEnumConstants()) {
                        if (enumValueMethodType.equals("Ordinal")) {
                            statement.setInt(1, enumValue.ordinal());
                        } else if (enumValueMethodType.equals("Id")) {
                            final Short idValue = (Short) enumClass.getMethod("getId").invoke(enumValue);
                            statement.setInt(1, idValue);
                        } else if (enumValueMethodType.equals("Status")) {
                            final Short statusValue = (Short) enumClass.getMethod("getStatus").invoke(enumValue);
                            statement.setInt(1, statusValue);
                        } else {
                            throw new RuntimeException(
                                    "Don't know how to handle an enum method type defined in line: " + line);
                        }
                        statement.setString(2, enumValue.name());

                        statement.execute();
                    }
                } catch (final Exception e) {
                    throw new RuntimeException("Loading enums types: " + line, e);
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Creating tables: " + e.getMessage(), e);
        }
    }

    @Override
    public void createConstraints(Connection connection, final Path ddlFile) {
        // do nothing

    }

    @Override
    public void loadOtherData(final Connection connection, final Path otherDataFilePath) {
        logger.info("\tNo other data to load...");
    }

    public Connection makeDBConnection(final Properties properties) throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("database.driver"));

        return DriverManager.getConnection(properties.getProperty("database.url"),
                                           properties.getProperty("database.user"),
                                           properties.getProperty("database.password"));

    }

    public Connection makeAdminConnection(final Properties properties) throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("database.driver"));

        return DriverManager.getConnection(properties.getProperty("database.admin.url"),
                properties.getProperty("database.user"),
                properties.getProperty("database.password"));
    }

    public void setup() {

    }

    @Override
    public String getPathToTableDDL() {
        return this.ddlFilePathString;
    }

    @Override
    public String getPathToEnumDML() {
        return this.enumDMLFilePathString;
    }

    @Override
    public String getDatabaseCreationString() {
        return this.databaseCreationString;
    }

    @Override
    public String getDatabaseConnectionConfigurationFilePath() {
        return this.dbConnectorFilePathString;
    }

    public void run() {
        final Properties properties = new Properties();
        final URL url = DefaultDatabaseCreator.class.getClassLoader()
                .getResource(getDatabaseConnectionConfigurationFilePath());
        if (url == null) {
            throw new RuntimeException(
                    "Unable to find db configuration file: " + getDatabaseConnectionConfigurationFilePath());
        }
        try {
            properties.load(url.openStream());
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Loading db properties at: " + getDatabaseConnectionConfigurationFilePath(), e);
        }

        try (final Connection adminConnection = makeAdminConnection(properties)) {
            try {
                adminConnection.setAutoCommit(false);

                logger.info("Creating database: " + getCatalogName());
                createDatabase(adminConnection);
            } catch (final Exception e) {
                throw e;
            }
        } catch (final Exception e) {
            throw new RuntimeException("Could not create database: ", e);
        }

        try (final Connection dbConnection = makeDBConnection(properties)) {
            try {
                dbConnection.setCatalog(getCatalogName());
                dbConnection.setAutoCommit(false);

                logger.info("Creating tables from DDL at: " + getPathToTableDDL());
                createTables(dbConnection, Paths.get(getPathToTableDDL()));

                logger.info("Loading enumeration data from file at: " + getPathToEnumDML());
                loadEnumData(dbConnection, Paths.get(getPathToEnumDML()));

                if (getPathToOtherDataFile() != null) {
                    logger.info("Loading other data from...");
                    loadOtherData(dbConnection, Paths.get(getPathToOtherDataFile()));
                }

                dbConnection.commit();
            } catch (final Exception e) {
                dbConnection.rollback();
                throw e;
            }
            logger.info("Database creation finished without errors.");
        } catch (final Exception e) {
            logger.error("Error creating databse, it will be dropped:  ", e);
            try (final Connection adminConnection = makeAdminConnection(properties)) {
                adminConnection.setAutoCommit(false);
                dropDatabase(adminConnection);
            } catch (final Exception ex) {
                throw new RuntimeException("WARNING: WARNING: WARNING: Could not drop database: ", ex);
            }
        }
    }

    @Override
    public void dropDatabase(final Connection adminConnection) {
        logger.info("Dropping database: " + getCatalogName());
        try {
            try (final PreparedStatement statement = adminConnection
                    .prepareStatement(getDatabaseDropString())) {
                statement.executeUpdate();
            }

        } catch (final Exception e) {
            throw new RuntimeException("Dropping database: " + getCatalogName(), e);
        }
    }

    @Override
    public String getPathToOtherDataFile() {
        return null;
    }

    @Override
    public String getConnectionStringUserSeparator() {
        return this.connectionStringUserSeparator;
    }

    @Override
    public String getConnectionStringPasswordSeparator() {
        return this.connectionStringPasswordSeparator;
    }

    @Override
    public String getCatalogName() {
        return this.catalogName;
    }

    @Override
    public String getDatabaseDropString() {
        return this.databaseDropString;
    }
}
