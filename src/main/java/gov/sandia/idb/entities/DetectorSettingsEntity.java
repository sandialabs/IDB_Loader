package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DetectorSettingsEntity {
    private Integer detectorSettingsId;
    private DetectorEntity detectorEntity;
    private Float detectorFWHM;
    private Float analyzerGain;
    private Float analyzerOffset;
    private String energyRange;
    private Integer numberOfChannels;

    public DetectorSettingsEntity() {
        super();
    }

    public Float getAnalyzerGain() {
        return this.analyzerGain;
    }

    public void setAnalyzerGain(Float analyzerGain) {
        this.analyzerGain = analyzerGain;
    }

    public Float getAnalyzerOffset() {
        return this.analyzerOffset;
    }

    public void setAnalyzerOffset(final Float analyzerOffset) {
        this.analyzerOffset = analyzerOffset;
    }

    public String getEnergyRange() {
        return this.energyRange;
    }

    public void setEnergyRange(final String energyRange) {
        this.energyRange = energyRange;
    }

    public Integer getNumberOfChannels() {
        return numberOfChannels;
    }

    public void setNumberOfChannels(Integer numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    public Integer getDetectorSettingsId() {
        return detectorSettingsId;
    }

    public void setDetectorSettingsId(Integer detectorSettingsId) {
        this.detectorSettingsId = detectorSettingsId;
    }

    public DetectorEntity getDetectorEntity() {
        return this.detectorEntity;
    }

    public void setDetectorEntity(final DetectorEntity detectorEntity) {
        this.detectorEntity = detectorEntity;
    }

    public Float getDetectorFWHM() {
        return detectorFWHM;
    }

    public void setDetectorFWHM(Float detectorFWHM) {
        this.detectorFWHM = detectorFWHM;
    }

    @Override
    public String toString() {
        return "DetectorSettingsEntity [detectorSettingsId=" + detectorSettingsId + ", detectorId=" + detectorEntity
                + ", detectorFWHM=" + detectorFWHM + ", analyzerGain=" + analyzerGain + ", energyRange=" + energyRange
                + ", numberOfChannels=" + numberOfChannels + "]";
    }

    final private static String NAME_VALUE_PAIR_KEY_GAIN = "configuration.detector.analyzer.gain";
    final private static String NAME_VALUE_PAIR_KEY_OFFSET = "configuration.detector.analyzer.offset";
    final private static String NAME_VALUE_PAIR_KEY_NUMBER_OF_CHANNELS = "configuration.detector.analyzer.number_of_channels";
    final private static String NAME_VALUE_PAIR_KEY_ENERGY_RANGE = "configuration.detector.analyzer.energy_range";
    final private static String NAME_VALUE_PAIR_KEY_FWHM = "configuration.detector.fwhm";
    final static private String UID_METADATA_NAME_VALUE_PAIR = "UID.metadata";

    final private static Set<DetectorSettingsEntity> cache = new HashSet<>();

    final private static String INSERT_ANALYZER = " INSERT INTO detector_settings_table( "
            + "   detector_id, analyzer_gain, analyzer_offset, "
            + "energy_range, number_of_channels, detector_fwhm ) "
            + "  VALUES( ?,?,?,?,?, ? );";
            
    final private static String SELECT_ANALYZER = " SELECT detector_settings_id "
            + " FROM detector_settings_table AS ds "
            + " WHERE detector_id = (? ) "
            + "   AND (analyzer_gain IS NULL AND analyzer_gain <=> (?)) "
            + "       OR (analyzer_gain IS NOT NULL AND ABS(analyzer_gain - (?)) < 0.00001) "
            + "   AND (analyzer_offset IS NULL AND analyzer_offset <=> (?)) "
            + "       OR (analyzer_offset IS NOT NULL AND ABS(analyzer_offset - (?)) < 0.00001) "
            + "   AND energy_range <=> (?) "
            + "   AND number_of_channels <=> (?) "
            + "   AND (detector_fwhm IS NULL AND detector_fwhm <=> (?)) "
            + "       OR (detector_fwhm IS NOT NULL AND ABS(detector_fwhm - (?)) < 0.00001) ";

    public static final DetectorSettingsEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        final DetectorSettingsEntity entity = DetectorSettingsEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Could not create or find Detector Settings Entity: " + nameValuePairs);
        }

        if (entity.getDetectorSettingsId() != null) {
            return entity;
        }

        try (final PreparedStatement statement = conn.prepareStatement(DetectorSettingsEntity.INSERT_ANALYZER,
                Statement.RETURN_GENERATED_KEYS)) {
            DetectorSettingsEntity.setFieldValuesIntoPreparedStatement(statement, entity, false);
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setDetectorSettingsId(key);
                    DetectorSettingsEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting detector settings form from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    public static final DetectorSettingsEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String uidMetadataString = nameValuePairs.get(DetectorSettingsEntity.UID_METADATA_NAME_VALUE_PAIR);

        // --------------------------------------------------
        // This is a known bug. The Detector Settings table should not have
        // a dependency on the detector id because detector settings values
        // come from the spectrum_metadata files while the detector values
        // come from the measurement_metadata files. To find which
        // detector made the settings, a database user should do a join
        // from detector settings to the spectrum_measurement join table
        // to the measurement table and finally to the detector table.
        // -----------------------------------------------------
        final DetectorEntity detectorEntity = MeasurementEntity.find(Integer.valueOf(uidMetadataString))
                .getConfigurationEntity()
                .getDetectorEntity();
        final DetectorSettingsEntity entity = new DetectorSettingsEntity();
        entity.setDetectorEntity(detectorEntity);
        final String analyzerGainString = nameValuePairs.get(DetectorSettingsEntity.NAME_VALUE_PAIR_KEY_GAIN);
        final String analyzerOffsetString = nameValuePairs.get(DetectorSettingsEntity.NAME_VALUE_PAIR_KEY_OFFSET);
        final String numberOfChannelsString = nameValuePairs
                .get(DetectorSettingsEntity.NAME_VALUE_PAIR_KEY_NUMBER_OF_CHANNELS);
        final String detectorFWHMString = nameValuePairs.get(DetectorSettingsEntity.NAME_VALUE_PAIR_KEY_FWHM);
        final String energyRangeString = nameValuePairs.get(DetectorSettingsEntity.NAME_VALUE_PAIR_KEY_ENERGY_RANGE);

        entity.setAnalyzerGain(analyzerGainString == null ? null : Float.valueOf(analyzerGainString));
        entity.setAnalyzerOffset(analyzerOffsetString == null ? null : Float.valueOf(analyzerOffsetString));
        entity.setNumberOfChannels(numberOfChannelsString == null ? null : Integer.valueOf(numberOfChannelsString));
        entity.setDetectorFWHM(detectorFWHMString == null ? null : Float.valueOf(detectorFWHMString));
        entity.setEnergyRange(energyRangeString);

        for (final DetectorSettingsEntity cachedEntity : DetectorSettingsEntity.cache) {
            if (DetectorSettingsEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // look in database for duplicate...
        try (final PreparedStatement statement = conn.prepareStatement(DetectorSettingsEntity.SELECT_ANALYZER)) {
            DetectorSettingsEntity.setFieldValuesIntoPreparedStatement(statement, entity, true);

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setDetectorSettingsId(rs.getInt("detector_settings_id"));
                    DetectorSettingsEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Selecting detector settings: " + nameValuePairs, e);
        }

        // not in cache, not in database, return without id...
        return entity;
    }

    public static final boolean haveEqualValues(final DetectorSettingsEntity first,
            final DetectorSettingsEntity second) {

        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else {
            if (second == null) {
                return false;
            }
        }

        if (!DetectorEntity.haveEqualValues(first.getDetectorEntity(),
                second.getDetectorEntity())) {
            return false;
        }

        if (first.getAnalyzerGain() == null) {
            if (second.getAnalyzerGain() != null) {
                return false;
            }
        } else if (!first.getAnalyzerGain().equals(second.getAnalyzerGain())) {
            return false;
        }

        if (first.getAnalyzerOffset() == null) {
            if (second.getAnalyzerOffset() != null) {
                return false;
            }
        } else if (!first.getAnalyzerOffset().equals(second.getAnalyzerOffset())) {
            return false;
        }

        if (first.getDetectorFWHM() == null) {
            if (second.getDetectorFWHM() != null) {
                return false;
            }
        } else if (!first.getDetectorFWHM().equals(second.getDetectorFWHM())) {
            return false;
        }

        if (first.getEnergyRange() == null) {
            if (second.getEnergyRange() != null) {
                return false;
            }
        } else if (!first.getEnergyRange().equals(second.getEnergyRange())) {
            return false;
        }

        if (first.getNumberOfChannels() == null) {
            if (second.getNumberOfChannels() != null) {
                return false;
            }
        } else if (!first.getNumberOfChannels().equals(second.getNumberOfChannels())) {
            return false;
        }

        return true;
    }

    private static void setFieldValuesIntoPreparedStatement(final PreparedStatement statement,
            final DetectorSettingsEntity entity,
            final boolean isSelect) {
        try {
            // if there isn't a detecotr id, then we want to throw an error...
            int index = 1;
            statement.setInt(index, entity.getDetectorEntity().getDetectorId());
            index++;

            if (entity.getAnalyzerGain() == null) {
                statement.setNull(index, Types.FLOAT);
            } else {
                statement.setFloat(index, entity.getAnalyzerGain());
            }
            index++;

            if (isSelect) {
                if (entity.getAnalyzerGain() == null) {
                    statement.setNull(index, Types.FLOAT);
                } else {
                    statement.setFloat(index, entity.getAnalyzerGain());
                }
                index++;    
            }

            if (entity.getAnalyzerOffset() == null) {
                statement.setNull(index, Types.FLOAT);
            } else {
                statement.setFloat(index, entity.getAnalyzerOffset());
            }
            index++; 

            if (isSelect) {
                if (entity.getAnalyzerOffset() == null) {
                    statement.setNull(index, Types.FLOAT);
                } else {
                    statement.setFloat(index, entity.getAnalyzerOffset());
                }
                index++; 
            }

            if (entity.getEnergyRange() == null) {
                statement.setNull(index, Types.INTEGER);
            } else {
                statement.setString(index, entity.getEnergyRange());
            }
            index++; 

            if (entity.getNumberOfChannels() == null) {
                statement.setNull(index, Types.INTEGER);
            } else {
                statement.setInt(index, entity.getNumberOfChannels());
            }
            index++; 

            if (entity.getDetectorFWHM() == null) {
                statement.setNull(index, Types.FLOAT);
            } else {
                statement.setFloat(index, entity.getDetectorFWHM());
            }
            index++;

            if (isSelect) {
                if (entity.getDetectorFWHM() == null) {
                    statement.setNull(index, Types.FLOAT);
                } else {
                    statement.setFloat(index, entity.getDetectorFWHM());
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Setting values into prepared statement: "
                    + entity, e);
        }
    }
}
