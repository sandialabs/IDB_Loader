
package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class SpectrumAcquisitionEntity {
    private Integer spectrumAcquistionId;
    private SpectrumMeasurementEntity spectrumMeasurementEntity;
    private String acquisitionDateTime;
    private Integer totalCounts;
    private Double realTimeSeconds;
    private Double liveTimeSeconds;
    private Double deadTime;
    private Double countRate;
    private Integer uidSpectrum;

    public Integer getSpectrumAcquistionId() {
        return spectrumAcquistionId;
    }

    public void setSpectrumAcquistionId(Integer spectrumAcquistionId) {
        this.spectrumAcquistionId = spectrumAcquistionId;
    }

    public SpectrumMeasurementEntity getSpectrumMeasurementEntity() {
        return spectrumMeasurementEntity;
    }

    public void setSpectrumMeasurementEntity(SpectrumMeasurementEntity spectrumMeasurementEntity) {
        this.spectrumMeasurementEntity = spectrumMeasurementEntity;
    }

    public String getAcquisitionDateTime() {
        return acquisitionDateTime;
    }

    public void setAcquisitionDateTime(final String acquisitionDateTime) {
        this.acquisitionDateTime = acquisitionDateTime;
    }

    public Integer getTotalCounts() {
        return totalCounts;
    }

    public void setTotalCounts(Integer totalCounts) {
        this.totalCounts = totalCounts;
    }

    public Double getRealTimeSeconds() {
        return realTimeSeconds;
    }

    public void setRealTimeSeconds(Double realTimeSeconds) {
        this.realTimeSeconds = realTimeSeconds;
    }

    public Double getLiveTimeSeconds() {
        return liveTimeSeconds;
    }

    public void setLiveTimeSeconds(Double liveTimeSeconds) {
        this.liveTimeSeconds = liveTimeSeconds;
    }

    public Double getDeadTime() {
        return deadTime;
    }

    public void setDeadTime(Double deadTime) {
        this.deadTime = deadTime;
    }

    public Double getCountRate() {
        return countRate;
    }

    public void setCountRate(Double countRate) {
        this.countRate = countRate;
    }

    public Integer getUidSpectrum() {
        return uidSpectrum;
    }

    public void setUidSpectrum(Integer uidSpectrum) {
        this.uidSpectrum = uidSpectrum;
    }

    @Override
    public String toString() {
        return "SpectrumAcquisitionEntity [spectrumAcquistionId=" + spectrumAcquistionId
                + ", spectrumMeasurementEntity=" + spectrumMeasurementEntity + ", acquisitionDateTime="
                + acquisitionDateTime + ", totalCounts=" + totalCounts + ", realTimeSeconds=" + realTimeSeconds
                + ", liveTimeSeconds=" + liveTimeSeconds + ", deadTime=" + deadTime + ", countRate=" + countRate
                + ", uidSpectrum=" + uidSpectrum + "]";
    }

    private static final String ACQUISTION_DATETIME_NAME_VALUE_PAIR = "configuration.acquisition.date";
    private static final String TOTAL_COUNTS_NAME_VALUE_PAIR = "configuration.acquisition.total_counts";
    private static final String REAL_TIME_NAME_VALUE_PAIR = "configuration.acquisition.real_time.seconds";
    private static final String DEAD_TIME_NAME_VALUE_PAIR = "configuration.acquisition.dead_time";
    private static final String LIVE_TIME_NAME_VALUE_PAIR = "configuration.acquisition.live_time.seconds";
    private static final String COUNT_RATE_NAME_VALUE_PAIR = "configuration.acquisition.count_rate";
    final static private String UID_SPECTRUM_NAME_VALUE_PAIR = "UID.spectrum";

    final private static Map<Integer, SpectrumAcquisitionEntity> cache = new HashMap<>();
    private static final String INSERT_SPECTRUM_ACQUISITION = " INSERT INTO spectrum_acquisition_table ( "
            + "   spectrum_measurement_id, uid_spectrum, "
            + "   acquisition_date_time, total_counts, real_time_seconds, "
            + "   live_time_seconds, dead_time, count_rate ) "
            + " VALUES ( ?,?,?,?,?, ?,?,? ); ";

    private static final String SELECT_SPECTRUM_ACQUISITION = " SELECT spectrum_acquisition_id "
            + " FROM spectrum_acquisition_table AS sat "
            + " WHERE spectrum_measurement_id = (?) "
            + "   AND uid_spectrum = (?) "
            + "   AND acquisition_date_time = (?) "
            + "   AND total_counts = (?) "
            + "   AND real_time_seconds = (?) "
            + "   AND live_time_seconds = (?) "
            + "   AND dead_time = (?) "
            + "   AND count_rate = (?) ";

    public static SpectrumAcquisitionEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SpectrumAcquisitionEntity entity = SpectrumAcquisitionEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make spectrum_acquisition: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getSpectrumAcquistionId() != null) {
            return entity;
        }

        // need to create a new entity...
        try (final PreparedStatement statement = conn.prepareStatement(
                SpectrumAcquisitionEntity.INSERT_SPECTRUM_ACQUISITION,
                Statement.RETURN_GENERATED_KEYS)) {

            SpectrumAcquisitionEntity.setFieldValuesIntoPreparedStatement(statement, entity);
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSpectrumAcquistionId(key);

                    // Note that the key is the uid spectrum so that
                    // the spectrum counts and spectrum md5 entities
                    // can find the spectrum and its database id...
                    SpectrumAcquisitionEntity.cache.put(entity.getUidSpectrum(), entity);
                    return entity;
                }
            }
            return null;
        } catch (final Exception e) {
            throw new RuntimeException("Inserting supplementary measurement values from pairs: " + nameValuePairs, e);
        }

    }

    static public SpectrumAcquisitionEntity find(final Integer uidSpectrum) {
        return SpectrumAcquisitionEntity.cache.get(uidSpectrum);
    }

    private static final SpectrumAcquisitionEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String acquisitionDateTimeString = nameValuePairs
                .get(SpectrumAcquisitionEntity.ACQUISTION_DATETIME_NAME_VALUE_PAIR);
        // final Instant acquisitionDateTime =
        // SpectrumAcquisitionEntity.parseAcquisitionDateTime(acquisitionDateTimeString);

        final String totalCountsString = nameValuePairs.get(SpectrumAcquisitionEntity.TOTAL_COUNTS_NAME_VALUE_PAIR);
        final Integer totalCounts = Integer.valueOf(totalCountsString);

        final String realTimeString = nameValuePairs.get(SpectrumAcquisitionEntity.REAL_TIME_NAME_VALUE_PAIR);
        final Double realTime = Double.valueOf(realTimeString);

        final String deadTimeString = nameValuePairs.get(SpectrumAcquisitionEntity.DEAD_TIME_NAME_VALUE_PAIR);
        final Double deadTime = Double.valueOf(deadTimeString);

        final String liveTimeString = nameValuePairs.get(SpectrumAcquisitionEntity.LIVE_TIME_NAME_VALUE_PAIR);
        final Double liveTime = Double.valueOf(liveTimeString);

        final String countRateString = nameValuePairs.get(SpectrumAcquisitionEntity.COUNT_RATE_NAME_VALUE_PAIR);
        final Double countRate = Double.valueOf(countRateString);

        final String uidSpectrumString = nameValuePairs.get(SpectrumAcquisitionEntity.UID_SPECTRUM_NAME_VALUE_PAIR);
        final Integer uidSpectrum = Integer.valueOf(uidSpectrumString.trim());

        final SpectrumAcquisitionEntity entity = new SpectrumAcquisitionEntity();
        entity.setSpectrumMeasurementEntity(SpectrumMeasurementEntity.load(conn, nameValuePairs));
        entity.setAcquisitionDateTime(acquisitionDateTimeString);
        entity.setCountRate(countRate);
        entity.setDeadTime(deadTime);
        entity.setLiveTimeSeconds(liveTime);
        entity.setRealTimeSeconds(realTime);
        entity.setTotalCounts(totalCounts);
        entity.setUidSpectrum(uidSpectrum);

        for (final SpectrumAcquisitionEntity cachedEntity : SpectrumAcquisitionEntity.cache.values()) {
            if (SpectrumAcquisitionEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(SpectrumAcquisitionEntity.SELECT_SPECTRUM_ACQUISITION)) {
            SpectrumAcquisitionEntity.setFieldValuesIntoPreparedStatement(statement,
                    entity);

            try (final ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {

                    // entity in the database has equal values except for id,
                    // so set the database id into entity and return...
                    entity.setSpectrumAcquistionId(rs.getInt("spectrum_acquisition_id"));

                    SpectrumAcquisitionEntity.cache.put(entity.getUidSpectrum(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Spectrum Acquisition Entity: " + entity, e);
        }
        // this acquisition entity has new values that need to be added to the
        // measurement table, so return the non-cached entity...
        return entity;
    }

    public static boolean haveEqualValues(final SpectrumAcquisitionEntity first,
            final SpectrumAcquisitionEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getSpectrumMeasurementEntity() == null) {
            if (second.getSpectrumMeasurementEntity() != null) {
                return false;
            }
        } else if (!SpectrumMeasurementEntity.haveEqualValues(first.getSpectrumMeasurementEntity(),
                second.getSpectrumMeasurementEntity())) {
            return false;
        }

        if (first.getAcquisitionDateTime() == null) {
            if (second.getAcquisitionDateTime() != null) {
                return false;
            }
        } else if (!first.getAcquisitionDateTime().equals(second.getAcquisitionDateTime())) {
            return false;
        }

        if (first.getUidSpectrum() == null) {
            if (second.getUidSpectrum() != null) {
                return false;
            }
        } else if (!first.getUidSpectrum().equals(second.getUidSpectrum())) {
            return false;
        }

        if (first.getCountRate() == null) {
            if (second.getCountRate() != null) {
                return false;
            }
        } else if (!first.getCountRate().equals(second.getCountRate())) {
            return false;
        }

        if (first.getDeadTime() == null) {
            if (second.getDeadTime() != null) {
                return false;
            }
        } else if (!first.getDeadTime().equals(second.getDeadTime())) {
            return false;
        }

        if (first.getLiveTimeSeconds() == null) {
            if (second.getLiveTimeSeconds() != null) {
                return false;
            }
        } else if (!first.getLiveTimeSeconds().equals(second.getLiveTimeSeconds())) {
            return false;
        }

        if (first.getRealTimeSeconds() == null) {
            if (second.getRealTimeSeconds() != null) {
                return false;
            }
        } else if (!first.getRealTimeSeconds().equals(second.getRealTimeSeconds())) {
            return false;
        }

        if (first.getTotalCounts() == null) {
            if (second.getTotalCounts() != null) {
                return false;
            }
        } else if (!first.getTotalCounts().equals(second.getTotalCounts())) {
            return false;
        }
        return true;
    }

    private static void setFieldValuesIntoPreparedStatement(final PreparedStatement statement,
            final SpectrumAcquisitionEntity entity) {
        try {
            // there should never be a spectrum without a measurement association...
            statement.setInt(1, entity.getSpectrumMeasurementEntity().getSpectrumMeasurementId());

            // there should always be a UID
            statement.setInt(2, entity.getUidSpectrum());

            if (entity.getAcquisitionDateTime() == null) {
                statement.setNull(3, Types.TIMESTAMP);
            } else {
                statement.setString(3, entity.getAcquisitionDateTime());
            }

            // there should always be a total counts...
            statement.setInt(4, entity.getTotalCounts());

            if (entity.getRealTimeSeconds() == null) {
                statement.setNull(5, Types.FLOAT);
            } else {
                statement.setFloat(5, entity.getRealTimeSeconds().floatValue());
            }

            if (entity.getLiveTimeSeconds() == null) {
                statement.setNull(6, Types.FLOAT);
            } else {
                statement.setFloat(6, entity.getLiveTimeSeconds().floatValue());
            }

            if (entity.getDeadTime() == null) {
                statement.setNull(7, Types.FLOAT);
            } else {
                statement.setFloat(7, entity.getDeadTime().floatValue());
            }

            if (entity.getCountRate() == null) {
                statement.setNull(8, Types.FLOAT);
            } else {
                statement.setFloat(8, entity.getCountRate().floatValue());
            }
        } catch (final Exception e) {
            throw new RuntimeException("Setting values into prepared statement: "
                    + entity, e);
        }
    }
}
