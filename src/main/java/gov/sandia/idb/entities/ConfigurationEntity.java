package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationEntity {

    private Integer configurationId;
    private DetectorEntity detectorEntity;
    private Float distance;
    private String distanceNotes;

    public Integer getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(Integer configurationId) {
        this.configurationId = configurationId;
    }

    public DetectorEntity getDetectorEntity() {
        return detectorEntity;
    }

    public void setDetectorEntity(DetectorEntity detectorEntity) {
        this.detectorEntity = detectorEntity;
    }

    public Float getDistance() {
        return distance;
    }

    public void setDistance(Float distance) {
        this.distance = distance;
    }

    public String getDistanceNotes() {
        return distanceNotes;
    }

    public void setDistanceNotes(String distanceNotes) {
        this.distanceNotes = distanceNotes;
    }

    final static private String DISTANCE_NAME_VALUE_PAIR = "configuration.distance";

    final static private Map<Integer, ConfigurationEntity> cache = new HashMap<>();

    final private static String INSERT_CONFIGURATION = " INSERT INTO configuration_table( "
            + "   detector_id, distance ) "
            + "  VALUES( ?,? );";

    final private static String SELECT_CONFIGURATION_BY_DETECTOR_ID_DISTANCE = " SELECT configuration_id, "
            + "   detector_id, distance  "
            + " FROM configuration_table AS t "
            + " WHERE t.detector_id = (?) "
            + "  AND ABS(t.distance - (?)) < 0.0001; ";

            final private static String SELECT_CONFIGURATION_BY_DETECTOR_ID_DISTANCE_NULL= " SELECT configuration_id, "
            + "   detector_id, distance  "
            + " FROM configuration_table AS t "
            + " WHERE t.detector_id = (?) "
            + "  AND t.distance IS NULL; ";

    static final public ConfigurationEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final ConfigurationEntity entity = ConfigurationEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Cannot find or create an Configuration entity.");
        }
        if (entity.getConfigurationId() != null) {
            return entity;
        }

        // Save the new entity to the database and put it in the
        // cache after we set it id values.

            try (final PreparedStatement statement = conn.prepareStatement(
                    ConfigurationEntity.INSERT_CONFIGURATION,
                    Statement.RETURN_GENERATED_KEYS)) {
                if (entity.getDetectorEntity() == null) {
                    statement.setNull(1, Types.INTEGER);
                } else {
                    statement.setInt(1, entity.getDetectorEntity().getDetectorId());
                }

                if (entity.getDistance() == null) {
                    statement.setNull(2, Types.FLOAT);
                } else {
                    statement.setFloat(2, entity.getDistance());
                }

                statement.executeUpdate();
                try (final ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        final Integer key = Integer.valueOf(rs.getInt(1));
                        if (rs.wasNull()) {
                            throw new RuntimeException("Key is null.");
                        }

                        entity.setConfigurationId(key);
                        ConfigurationEntity.cache.put(key, entity);
                        return entity;
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException("Inserting configuration: " + nameValuePairs, e);
            }


        return null;
    }

    static final public ConfigurationEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final ConfigurationEntity entity = new ConfigurationEntity();
        entity.setDetectorEntity(DetectorEntity.load(conn, nameValuePairs));

        final String distanceString = nameValuePairs.get(ConfigurationEntity.DISTANCE_NAME_VALUE_PAIR);
        entity.setDistance(distanceString == null ? null : Float.valueOf(distanceString));
        for (final ConfigurationEntity cachedEntity : ConfigurationEntity.cache.values()) {
            if (ConfigurationEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // now check the database...
        final String preparedStatement = entity.getDistance() == null ? 
                                 ConfigurationEntity.SELECT_CONFIGURATION_BY_DETECTOR_ID_DISTANCE_NULL :
                                 ConfigurationEntity.SELECT_CONFIGURATION_BY_DETECTOR_ID_DISTANCE;
        try (final PreparedStatement statement = conn
                .prepareStatement(preparedStatement)) {
            statement.setInt(1, entity.getDetectorEntity().getDetectorId());
            if (entity.getDistance() != null) {
                statement.setFloat(2, entity.getDistance());
            }

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setConfigurationId(rs.getInt("configuration_id"));

                    ConfigurationEntity.cache.put(entity.getConfigurationId(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Analyzer for name: " + entity, e);
        }

        // this configuration entity has new values that need to be added to the
        // configuration table, so return the non-cached entity...
        return entity;
    }

    /**
     * 
     * @param first
     * @param second
     * @return
     */
    public static boolean haveEqualValues(final ConfigurationEntity first,
            final ConfigurationEntity second) {

        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getDistance() == null) {
            if (second.getDistance() != null) {
                return false;
            }
        } else if (!first.getDistance().equals(second.getDistance())) {
            return false;
        }

        if (first.getDistanceNotes() == null) {
            if (second.getDistanceNotes() != null) {
                return false;
            }
        } else if (!first.getDistanceNotes().equals(second.getDistanceNotes())) {
            return false;
        }

        // if (first.getDistance_string() == null) {
        //     if (second.getDistanceNotes() != null) {
        //         return false;
        //     }
        // } else if (!first.getDistance_string().equals(second.getDistance_string())) {
        //     return false;
        // }

        if (first.getDetectorEntity() == null) {
            if (second.getDetectorEntity() != null) {
                return false;
            }
        } else if (!DetectorEntity.haveEqualValues(first.getDetectorEntity(), second.getDetectorEntity())) {
            return false;
        }

        return true;
    }

}
