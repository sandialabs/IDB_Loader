package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class SupplementaryMeasurementEntity {

    private Integer supplementaryMeasurementId;
    private String measurementDetectorSize;
    private String measurementElectronics;
    private String measurementGeometry;

    public Integer getSupplementaryMeasurementId() {
        return supplementaryMeasurementId;
    }

    public void setSupplementaryMeasurementId(Integer supplementaryMeasurementId) {
        this.supplementaryMeasurementId = supplementaryMeasurementId;
    }

    public String getMeasurementDetectorSize() {
        return measurementDetectorSize;
    }

    public void setMeasurementDetectorSize(String measurementDetectorSize) {
        this.measurementDetectorSize = measurementDetectorSize;
    }

    public String getMeasurementElectronics() {
        return measurementElectronics;
    }

    public void setMeasurementElectronics(String measurementElectronics) {
        this.measurementElectronics = measurementElectronics;
    }

    public String getMeasurementGeometry() {
        return measurementGeometry;
    }

    public void setMeasurementGeometry(String measurementGeometry) {
        this.measurementGeometry = measurementGeometry;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((supplementaryMeasurementId == null) ? 0 : supplementaryMeasurementId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SupplementaryMeasurementEntity other = (SupplementaryMeasurementEntity) obj;
        if (supplementaryMeasurementId == null) {
            if (other.supplementaryMeasurementId != null) {
                return false;
            } else {
                return SupplementaryMeasurementEntity.haveEqualValues(this, other);
            }
        } else if (!supplementaryMeasurementId.equals(other.supplementaryMeasurementId))
            return false;
        return true;
    }

    final private static String MEASUREMENT_DETECTOR_SIZE_NAME_VALUE_PAIR_KEY = "supplementary.detector_size";
    final private static String MEASUREMENT_ELECTRONICS_NAME_VALUE_PAIR_KEY = "supplementary.electronics";
    final private static String MEASUREMENT_GEOMETRY_NAME_VALUE_PAIR_KEY = "supplementary.measurement_geometry";

    final private static Map<Integer, SupplementaryMeasurementEntity> cache = new HashMap<>();
    final private static String INSERT_SUPPLEMENTARY_MEASUREMENT = " INSERT INTO supplementary_measurement_table( "
            + "   measurement_detector_size, measurement_electronics, measurement_geometry  ) "
            + "  VALUES( ?,?,? );";

    final private static String SELECT_BY_VALUES = " SELECT supplementary_measurement_id, "
            + "     measurement_detector_size, measurement_electronics, measurement_geometry "
            + " FROM supplementary_measurement_table AS m "
            + " WHERE m.measurement_detector_size <=> (?) "
            + "   AND m.measurement_electronics <=> (?) "
            + "   AND m.measurement_geometry <=> (?); ";

    public static final SupplementaryMeasurementEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SupplementaryMeasurementEntity entity = find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make supplementary_measurement: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getSupplementaryMeasurementId() != null) {
            return entity;
        }

        // need to create a new entity...
        try (final PreparedStatement statement = conn.prepareStatement(
                SupplementaryMeasurementEntity.INSERT_SUPPLEMENTARY_MEASUREMENT,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getMeasurementDetectorSize() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getMeasurementDetectorSize());
            }
            if (entity.getMeasurementElectronics() == null) {
                statement.setNull(2, Types.VARCHAR);
            } else {
                statement.setString(2, entity.getMeasurementElectronics());
            }

            if (entity.getMeasurementGeometry() == null) {
                statement.setNull(3, Types.VARCHAR);
            } else {
                statement.setString(3, entity.getMeasurementGeometry());
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSupplementaryMeasurementId(key);
                    SupplementaryMeasurementEntity.cache.put(entity.getSupplementaryMeasurementId(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting supplementary measurement values from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    private static final SupplementaryMeasurementEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {
        final SupplementaryMeasurementEntity entity = new SupplementaryMeasurementEntity();

        final String measurementGeometry = nameValuePairs
                .get(SupplementaryMeasurementEntity.MEASUREMENT_GEOMETRY_NAME_VALUE_PAIR_KEY);
        entity.setMeasurementDetectorSize(
                nameValuePairs.get(SupplementaryMeasurementEntity.MEASUREMENT_DETECTOR_SIZE_NAME_VALUE_PAIR_KEY));
        entity.setMeasurementElectronics(
                nameValuePairs.get(SupplementaryMeasurementEntity.MEASUREMENT_ELECTRONICS_NAME_VALUE_PAIR_KEY));
        entity.setMeasurementGeometry(measurementGeometry);

        // if the values are already in the cache, then
        // it is a duplicate and we want to use the cached values
        for (final SupplementaryMeasurementEntity cachedEntity : SupplementaryMeasurementEntity.cache.values()) {
            if (SupplementaryMeasurementEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // now look in the database and add to the cache if there is one in the db
        // already...
        try (final PreparedStatement statement = conn
                .prepareStatement(SupplementaryMeasurementEntity.SELECT_BY_VALUES)) {
            if (entity.getMeasurementDetectorSize() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getMeasurementDetectorSize());
            }
            if (entity.getMeasurementElectronics() == null) {
                statement.setNull(2, Types.VARCHAR);
            } else {
                statement.setString(2, entity.getMeasurementElectronics());
            }
            if (entity.getMeasurementGeometry() == null) {
                statement.setNull(3, Types.VARCHAR);
            } else {
                statement.setString(3, entity.getMeasurementGeometry());
            }
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setSupplementaryMeasurementId(rs.getInt("supplementary_measurement_id"));
                    SupplementaryMeasurementEntity.cache.put(entity.getSupplementaryMeasurementId(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Looking up supplementary measurement: " + entity, e);
        }
        // this entity has new values that need to be added to the
        // attenuator table, so return the non-cached entity...
        return entity;
    }

    public static boolean haveEqualValues(final SupplementaryMeasurementEntity first,
            final SupplementaryMeasurementEntity second) {
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

        if (first.getMeasurementDetectorSize() == null) {
            if (second.getMeasurementDetectorSize() != null) {
                return false;
            }
        } else if (!first.getMeasurementDetectorSize().equals(second.getMeasurementDetectorSize())) {
            return false;
        }

        if (first.getMeasurementElectronics() == null) {
            if (second.getMeasurementElectronics() != null) {
                return false;
            }
        } else if (!first.getMeasurementElectronics().equals(second.getMeasurementElectronics())) {
            return false;
        }

        if (first.getMeasurementGeometry() == null) {
            if (second.getMeasurementGeometry() != null) {
                return false;
            }
        } else if (!first.getMeasurementGeometry().equals(second.getMeasurementGeometry())) {
            return false;
        }

        return true;
    }

}
