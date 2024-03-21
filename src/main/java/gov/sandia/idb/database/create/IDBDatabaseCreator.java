package gov.sandia.idb.database.create;

import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IDBDatabaseCreator extends DefaultDatabaseCreator {
    final private static Logger logger = LoggerFactory.getLogger(IDBDatabaseCreator.class);

    public IDBDatabaseCreator(final String databasePropertyFilePath) {
        super(databasePropertyFilePath);
    }

    public final static void main(final String[] args) {
        logger.info("Creating database from properties file located at: " + args[0]);
        try {
            final IDBDatabaseCreator databaseCreator = new IDBDatabaseCreator(args[0]);
            databaseCreator.run();
            logger.info("IDB Database Created..." + databaseCreator.getDatabaseConnectionConfigurationFilePath());
        } catch (final Exception e) {
            throw new RuntimeException("Could not create database: ", e);
        } 
    }

    @Override
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
            throw new RuntimeException("Could not create IDB database: ", e);
        }

        try (final Connection idbConnection = makeDBConnection(properties)) {
            try {
                idbConnection.setCatalog(getCatalogName());
                idbConnection.setAutoCommit(false);

                logger.info("Creating tables from DDL at: " + getPathToTableDDL());
                createTables(idbConnection, Paths.get(getPathToTableDDL()));

                logger.info("Loading enumeration data from file at: " + getPathToEnumDML());
                loadEnumData(idbConnection, Paths.get(getPathToEnumDML()));

                if (getPathToOtherDataFile() != null) {
                    logger.info("Loading other data from...");
                    loadOtherData(idbConnection, Paths.get(getPathToOtherDataFile()));
                }

                idbConnection.commit();
            } catch (final Exception e) {
                idbConnection.rollback();
                throw e;
            }
            logger.info("Database creation finished without errors.");
        } catch (final Exception e) {
            logger.error("Error creating databse, it will be dropped:  ", e);
            try (final Connection adminConnection = makeAdminConnection(properties)) {
                adminConnection.setAutoCommit(false);
                dropDatabase(adminConnection);
            } catch (final Exception ex) {
                throw new RuntimeException("WARNING: WARNING: WARNING: Could not drop IDB database: ", ex);
            }
        }
    }

}
