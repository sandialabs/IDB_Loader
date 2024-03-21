package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SpectrumMeasurementEntity {

    private Integer spectrumMeasurementId;
    private DetectorSettingsEntity detectorSettingsEntity;
    private MeasurementEntity measurementEntity;

    public Integer getSpectrumMeasurementId() {
        return spectrumMeasurementId;
    }

    public void setSpectrumMeasurementId(Integer spectrumMeasurementId) {
        this.spectrumMeasurementId = spectrumMeasurementId;
    }

    public DetectorSettingsEntity getDetectorSettingsEntity() {
        return this.detectorSettingsEntity;
    }

    public void setDetectorSettingsEntity(final DetectorSettingsEntity detectorSettingsEntity) {
        this.detectorSettingsEntity = detectorSettingsEntity;
    }

    public MeasurementEntity getMeasurementEntity() {
        return measurementEntity;
    }

    public void setMeasurementEntity(MeasurementEntity measurementEntity) {
        this.measurementEntity = measurementEntity;
    }

    final static private String UID_METADATA_NAME_VALUE_PAIR = "UID.metadata";

    final private static Map<Integer, SpectrumMeasurementEntity> cache = new HashMap<>();
    private static final String INSERT_SPECTRUM_MEASUREMENT = " INSERT INTO spectrum_measurement_table ( "
            + "   measurement_id, "
            + "   detector_settings_id ) "
            + " VALUES ( ?,? ); ";

    private static final String SELECT_SPECTRUM_MEASUREMENT = " SELECT spectrum_measurement_id "
            + " FROM spectrum_measurement_table AS sm "
            + "  WHERE measurement_id = (?) "
            + "   AND  detector_settings_id = (?); ";

    public static SpectrumMeasurementEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SpectrumMeasurementEntity entity = SpectrumMeasurementEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make spectrum_measurement: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getSpectrumMeasurementId() != null) {
            return entity;
        }

        // need to create a new entity. No need to check
        // for null values during insert becasue there should
        // always be a measurement_id and a detector_settings_id.
        // If there isn't, an exception should be thrown.
        try (final PreparedStatement statement = conn.prepareStatement(
                SpectrumMeasurementEntity.INSERT_SPECTRUM_MEASUREMENT,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, entity.getMeasurementEntity().getMeasurementId());
            statement.setInt(2, Integer.valueOf(entity.getDetectorSettingsEntity().getDetectorSettingsId()));

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSpectrumMeasurementId(key);

                    SpectrumMeasurementEntity.cache.put(key, entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting spectrum measurement values from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    private static final SpectrumMeasurementEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String uidMetadataString = nameValuePairs.get(SpectrumMeasurementEntity.UID_METADATA_NAME_VALUE_PAIR);

        final SpectrumMeasurementEntity entity = new SpectrumMeasurementEntity();
        entity.setMeasurementEntity(MeasurementEntity.find(Integer.valueOf(uidMetadataString)));
        entity.setDetectorSettingsEntity(DetectorSettingsEntity.load(conn, nameValuePairs));

        for (final SpectrumMeasurementEntity cachedEntity : SpectrumMeasurementEntity.cache.values()) {
            if (SpectrumMeasurementEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(SpectrumMeasurementEntity.SELECT_SPECTRUM_MEASUREMENT)) {
                    statement.setInt(1, entity.getMeasurementEntity().getMeasurementId());
                    statement.setInt(2, Integer.valueOf(entity.getDetectorSettingsEntity().getDetectorSettingsId()));
        

            try (final ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {

                    // entity in the database has equal values except for id,
                    // so set the database id into entity and return...
                    entity.setSpectrumMeasurementId((rs.getInt("spectrum_measurement_id")));

                    SpectrumMeasurementEntity.cache.put(entity.getSpectrumMeasurementId(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Certificate: " + entity, e);
        }

        // this measurement entity has new values that need to be added to the
        // measurement table, so return the non-cached entity...
        return entity;
    }

    public static boolean haveEqualValues(final SpectrumMeasurementEntity first,
            final SpectrumMeasurementEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getMeasurementEntity() == null) {
            if (second.getMeasurementEntity() != null) {
                return false;
            }
        } else if (!MeasurementEntity.haveEqualValues(first.getMeasurementEntity(),
                second.getMeasurementEntity())) {
            return false;
        }

        if (first.getDetectorSettingsEntity() == null) {
            if (second.getDetectorSettingsEntity() != null) {
                return false;
            }
        } else if (!DetectorSettingsEntity.haveEqualValues(first.getDetectorSettingsEntity(),
                second.getDetectorSettingsEntity())) {
            return false;
        }
        return true;
    }

}
