package gov.sandia.idb.database.create;

import java.nio.file.Path;
import java.sql.Connection;

/**
 * Set up the IDB database tables,
 * load the fixed data (e.g., location
 * data, types), and then add the constraints.
 * 
 * ALL methods take a connection to the
 * database and all methods assume that the
 * caller is responsible for transactions.  Any
 * error local will be caught and re-thrown as
 * a run-time exception so if the user wants to
 * do a roll-back, the user needs to catch all
 * Exceptions.
 * 
 * Make an interface so if some strange db
 * implementation isn't happy with normal
 * JDBC versions of create tables or constraints
 * we can write a special version.
 * 
 * @author srschwa
 *
 */
public interface DatabaseCreator {

    public void createDatabase(final Connection connection);
    public void dropDatabase(final Connection connection);
    public void createTables(final Connection connection,
            final Path ddlForTableFile);
    public void loadEnumData(final Connection connection,
            final Path dataForTypeAndStateFile);
    public void createConstraints(final Connection connection,
                                  final Path ddlForConstraintsFile);

    public void loadOtherData(final Connection connection, final Path otherDataFilePath);  
    public String getPathToTableDDL();
    public String getPathToEnumDML();
    public String getPathToOtherDataFile();
    public String getDatabaseCreationString();
    public String getDatabaseConnectionConfigurationFilePath();
    public String getConnectionStringUserSeparator();
    public String getConnectionStringPasswordSeparator();
    public String getCatalogName();
    public String getDatabaseDropString();
}
