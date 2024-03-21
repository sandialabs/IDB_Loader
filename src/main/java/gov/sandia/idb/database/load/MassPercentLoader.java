package gov.sandia.idb.database.load;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.sandia.idb.entities.CertificateEntity;
import gov.sandia.idb.entities.ElementEntity;
import gov.sandia.idb.entities.LoadHistoryEntity;
import gov.sandia.idb.entities.MeasurementEntity;
import gov.sandia.idb.entities.SourceTypeEntity;
import gov.sandia.idb.entities.SpectrumAcquisitionEntity;
import gov.sandia.idb.entities.SpectrumChecksumEntity;
import gov.sandia.idb.entities.SpectrumCountsEntity;
import gov.sandia.idb.entities.SpectrumMeasurementEntity;

/**
 * 
 * @author srschwa
 *
 */
public class MassPercentLoader {
    private static Logger logger = LoggerFactory.getLogger(MassPercentLoader.class);

    final private static String ELEMENTS_CSV_FILE_PATH = "elements.csv.file.path";
    final private static String SOURCE_TYPE = "source.type";
    final private static String PROVENANCE_METADATA_FILE_PATH = "provenance.metadata.file.path";
    final private static String SOURCE_METADATA_FILE_PATH = "source.metadata.file.path";
    final private static String SPECTRUM_METADATA_FILE_PATH = "spectrum.metadata.file.path";
    final private static String MEASUREMENT_METADATA_FILE_PATH = "measurement.metadata.file.path";
    final private static String SPECTRUM_COUNTS_FILE_PATH = "spectrum.counts.file.path";
    final private static String SPECTRUM_CHECKSUM_FILE_PATH = "spectrum.checksum.file.path";

    final private static String DATABASE_DRIVER_CLASS = "database.driver";

    final public static void main(final String[] args) {

        final MassPercentLoader massPercentLoader = new MassPercentLoader();
        massPercentLoader.load(args[0]);

        logger.info("Load finished without errors: ");
    }

    public MassPercentLoader() {

    }

    public void load(final String configurationFilePath) {
        logger.info("Loading from configuration file:  " + configurationFilePath);

        final Properties properties = new Properties();
        final URL url = MassPercentLoader.class.getClassLoader().getResource(configurationFilePath);
        if (url == null) {
            throw new RuntimeException("Unable to find configuration file: " + configurationFilePath);
        }
        try {
            properties.load(url.openStream());
        } catch (final Exception e) {
            throw new RuntimeException("Reading config file: " + configurationFilePath, e);
        }

        logger.info("Load history description: "
                + properties.getProperty(LoadHistoryEntity.NAME_VALUE_PAIR_LOAD_HISTORY_DESCRIPTION));
        logger.info("Source type that will be loaded: "
                + properties.getProperty(MassPercentLoader.SOURCE_TYPE));
        logger.info("Load Provenance data from CSV File: "
                + properties.getProperty(MassPercentLoader.PROVENANCE_METADATA_FILE_PATH));
        logger.info("Load Source Meta data from CSV File:: "
                + properties.getProperty(MassPercentLoader.SOURCE_METADATA_FILE_PATH));
        logger.info("Load Measurement Meta data from CSV File:: "
                + properties.getProperty(MassPercentLoader.MEASUREMENT_METADATA_FILE_PATH));
        logger.info("Load Spectrum Meta data from CSV File:: "
                + properties.getProperty(MassPercentLoader.SPECTRUM_METADATA_FILE_PATH));
        logger.info("Load Spectrum Counts Meta data from CSV File:: "
                + properties.getProperty(MassPercentLoader.SPECTRUM_COUNTS_FILE_PATH));
        logger.info("Load Spectrum Checksum Meta data from CSV File:: "
                + properties.getProperty(MassPercentLoader.SPECTRUM_CHECKSUM_FILE_PATH));

        logger.info("Using database driver: " + properties.getProperty(MassPercentLoader.DATABASE_DRIVER_CLASS));

        this.load(properties);
    }

    public void load(final Properties properties) {
        try {
            Class.forName(properties.getProperty(MassPercentLoader.DATABASE_DRIVER_CLASS));
        } catch (final Exception e) {
            throw new RuntimeException(
                    "loading database driver: " + properties.getProperty(MassPercentLoader.DATABASE_DRIVER_CLASS));
        }

        try {

            try (final Connection conn = makeIDBConnection(properties)) {
                logger.info("Loaded connection...");
                conn.setAutoCommit(false);

                try {
                    loadElementData(conn,
                            Paths.get(properties.getProperty(MassPercentLoader.ELEMENTS_CSV_FILE_PATH)));
                    loadSourceTypeData(conn);
                    loadData(conn, properties);
                    conn.commit();
                } catch (final Exception e) {
                    conn.rollback();
                    throw new RuntimeException("Loading data, will rollback: ", e);
                }
            } catch (final Exception e) {
                throw new RuntimeException("Loading data: ", e);
            }
        } catch (final Exception e) {
            throw new RuntimeException("load data", e);
        }
    }

    public void loadData(final Connection conn, final Properties properties) {

        final String sourceType = properties.getProperty(MassPercentLoader.SOURCE_TYPE).toUpperCase();

        // load provenance metadata ...
        final Path provenanceMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.PROVENANCE_METADATA_FILE_PATH));

        final Set<Map<String, String>> loadHistoryNameValuePairList = new HashSet<>();
        for (final Map<String, String> nameValuePairs : this.readMetadaData(provenanceMetadataFile)) {
            nameValuePairs.put(LoadHistoryEntity.NAME_VALUE_PAIR_LOAD_HISTORY_DESCRIPTION,
                    properties.getProperty(LoadHistoryEntity.NAME_VALUE_PAIR_LOAD_HISTORY_DESCRIPTION));
                    nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            LoadHistoryEntity.load(conn, nameValuePairs);
            loadHistoryNameValuePairList.add(nameValuePairs);
        }

        // load source metadata
        // NOTE: The load history name value pairs are added to
        //  the source name value pairs from the CSV file so that
        //  during the load, the Certificate Entity can find the
        //  load history row that it belongs to...
        final Path sourceMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.SOURCE_METADATA_FILE_PATH));

        for (final Map<String, String> nameValuePairs : this.readMetadaData(sourceMetadataFile)) {
            nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            
            for (final Map<String, String> loadHistoryNameValuePairs : loadHistoryNameValuePairList) {
                nameValuePairs.putAll(loadHistoryNameValuePairs);
            }

            CertificateEntity.load(conn, nameValuePairs);
        }

        // load measurement metadata (depends on source)...
        final Path measurementMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.MEASUREMENT_METADATA_FILE_PATH));
        for (final Map<String, String> nameValuePairs : this.readMetadaData(measurementMetadataFile)) {
            if (nameValuePairs == null || nameValuePairs.size() == 0) {
                continue;
            }
            nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            MeasurementEntity.load(conn, nameValuePairs);
        }

        // load spectra metadata (depends on source and metadata)...
        final Path spectrumMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.SPECTRUM_METADATA_FILE_PATH));
        for (final Map<String, String> nameValuePairs : this.readMetadaData(spectrumMetadataFile)) {
            if (nameValuePairs == null || nameValuePairs.size() == 0) {
                continue;
            }
            nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            SpectrumMeasurementEntity.load(conn, nameValuePairs);
            SpectrumAcquisitionEntity.load(conn, nameValuePairs);
        }

        // load spectra count metadata (depends on source, metadata, spectrum)...
        final Path spectrumCountsMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.SPECTRUM_COUNTS_FILE_PATH));
        for (final Map<String, String> nameValuePairs : this.readMetadaData(spectrumCountsMetadataFile)) {
            if (nameValuePairs == null || nameValuePairs.size() == 0) {
                continue;
            }
            nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            SpectrumCountsEntity.load(conn, nameValuePairs);
        }

        final Path spectrumChecksumMetadataFile = Paths
                .get(properties
                        .getProperty(MassPercentLoader.SPECTRUM_CHECKSUM_FILE_PATH));
        for (final Map<String, String> nameValuePairs : this.readMetadaData(spectrumChecksumMetadataFile)) {
            if (nameValuePairs == null || nameValuePairs.size() == 0) {
                continue;
            }
            nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, sourceType);
            SpectrumChecksumEntity.load(conn, nameValuePairs);
        }

    }

    private List<Map<String, String>> readMetadaData(final Path metadataCSVFilePath) {
        logger.info("Reading metadata from: " + metadataCSVFilePath);
        final List<Map<String, String>> nameValuePairsList = new ArrayList<>();
        try {
            List<String> headerList = new ArrayList<>();
            for (final String line : Files.readAllLines(metadataCSVFilePath)) {

                Map<String, String> nameValuePairs = new HashMap<>();
                final String[] values = parseLine(line);
                if (values.length == 0) {
                    continue;
                } else if (values[0].toUpperCase().startsWith("UID")) {
                    logger.info("Loading header data: " + line);
                    headerList = Arrays.asList(values);
                } else {
                    logger.info("Loading metadata: " + line);
                    for (int i = 0; i < values.length; i++) {
                        nameValuePairs.put(headerList.get(i), parseValue(values[i]));
                    }
                    nameValuePairsList.add(nameValuePairs);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Loading metadata: ", e);
        }
        return nameValuePairsList;
    }

    /**
     * Parse input string value to:
     * 1. Trim leading/trailing white space
     * 2. Return NULL if value is the string UNKNOWN
     * or if the string is a magic flag value. Magic
     * flags are always negative integers. This method
     * assumes there are no other values that are pure
     * negative integer.
     * 
     * @param value
     * @return
     */
    private String parseValue(final String value) {
        if (value == null) {
            return value;
        }
        final String trimmedValue = value.trim();

        if (trimmedValue.isEmpty()) {
            return null;
        }

        // Set any unknown field to be null
        if (trimmedValue.equalsIgnoreCase("UNKNOWN")) {
            return null;
        }

        // if it doesn't start with a minus sign, it
        // can't be a magic number so just return
        // the trimmed input value.
        if (!trimmedValue.startsWith("-")) {
            return trimmedValue;
        }

        // Some of the US flags are float values, so
        // remove any period.
        for (int i = 1; i < trimmedValue.length(); i++) {
            final char c = trimmedValue.charAt(i);
            if (!(Character.isDigit(c)) && (c != '.')) {
                return trimmedValue;
            }
        }

        // otherwise, we don't know
        // if it is magic or not, so 
        // skip it.
        return null;
    }

    private String[] parseLine(final String line) {
        if (line.startsWith("#")) {
            return new String[0];
        }

        if (line.contains("\"")) {
            return parseLineWithQuotes(line);
        } else if (line.contains(",[")) {
            int firstComma = line.indexOf(",");
            final String[] splits = new String[2];
            splits[0] = line.substring(0, firstComma);
            splits[1] = line.substring(firstComma + 1);
            return splits;
        } else {
            return line.split(",");
        }
    }

    private String[] parseLineWithQuotes(final String line) {
        final List<String> parsedSplits = new ArrayList<>();

        boolean insideQuote = false;
        final StringBuilder sb = new StringBuilder();
        for (final String split : line.split(",")) {
            // if this is quoted then set a flag so
            // we can add everything to this value
            // until we find the next quote mark
            if (split.startsWith("\"")) {
                if (insideQuote) {
                    throw new RuntimeException("New quote inside quoted string...");
                }
                if (sb.length() != 0) {
                    throw new RuntimeException("Another quote found before end....");
                }
                insideQuote = Boolean.TRUE;
                // always drop the quote mark...
                sb.append(split.substring(1));
            } else if (insideQuote) {
                if (split.endsWith("\"")) {
                    insideQuote = Boolean.FALSE;
                    sb.append(split.substring(0, split.length() - 1));
                    parsedSplits.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(split);
                }
            } else {
                // a normal non-quoted split, so just add it
                parsedSplits.add(split);
            }
        }

        return parsedSplits.toArray(new String[parsedSplits.size()]);
    }

    private void loadElementData(final Connection conn,
            final Path configurationFilePath) {

        for (final ElementEntity elementEntity : createElementDataFromConfigurationFile(configurationFilePath)) {
            ElementEntity.load(conn, elementEntity.getElementName(), elementEntity.getElementSymbol());
        }
    }

    private void loadSourceTypeData(final Connection conn) {

        final Map<String, String> nameValuePairs = new HashMap<>();
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, "PU");
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_DESCRIPTION_NAME_VALUE_PAIR, "Plutonium");
        SourceTypeEntity.load(conn, nameValuePairs);
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, "MOX");
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_DESCRIPTION_NAME_VALUE_PAIR, "Mixed Oxide");
        SourceTypeEntity.load(conn, nameValuePairs);
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR, "U");
        nameValuePairs.put(SourceTypeEntity.SOURCE_TYPE_DESCRIPTION_NAME_VALUE_PAIR, "Uranium");
        SourceTypeEntity.load(conn, nameValuePairs);
    }

    private Set<ElementEntity> createElementDataFromConfigurationFile(final Path configurationFilePath) {
        final Set<ElementEntity> elementEntitySet = new HashSet<>();
        try {
            for (final String line : Files.readAllLines(configurationFilePath)) {
                final String[] values = parseLine(line);
                if (values.length == 0) {
                    continue;
                }
                // first non-comment line is column headers, skip...
                if (values[0].startsWith("Element")) {
                    continue;
                }
                final ElementEntity entity = new ElementEntity(values[0].toUpperCase().trim(),
                        values[1].toUpperCase().trim());
                elementEntitySet.add(entity);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Reading element file " + configurationFilePath, e);
        }

        return elementEntitySet;

    }

    public Connection makeIDBConnection(final Properties properties) throws ClassNotFoundException, SQLException {
        final String database_user = properties.getProperty("database.user");
        String database_pw = properties.getProperty("database.password");
        if (database_pw == null || database_pw.isEmpty()) {
            database_pw = String.valueOf(System.console().readPassword("Enter mariadb pw for user: " + database_user));
        }
        return DriverManager.getConnection(properties.getProperty("database.url"),
                database_user,
                database_pw);
    }
}
