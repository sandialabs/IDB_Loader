package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class DetectorGeometryEntity {
    private Integer geometryId;
    private String geometryName;
    
    public DetectorGeometryEntity() {
        
    }
    
    public DetectorGeometryEntity(String geometryName) {
        super();
        this.geometryName = geometryName;

    }

    public DetectorGeometryEntity(Integer geometryId, String geometryName) {
        super();
        this.geometryId = geometryId;
        this.geometryName = geometryName;

    }

    public Integer getGeometryId() {
        return geometryId;
    }

    public void setGeometryId(Integer geometryId) {
        this.geometryId = geometryId;
    }

    public String getGeometryName() {
        return geometryName;
    }

    public void setGeometryName(String geometryName) {
        this.geometryName = geometryName;
    }

    @Override
    public String toString() {
        return "DetectorGeometryEntity [geometryId=" + geometryId + ", geometryName=" + geometryName
                 + "]";
    }
   
    final private static String NAME_VALUE_PAIR_KEY = "configuration.detector.geometry";
    final private static Map<String, DetectorGeometryEntity> cache = new HashMap<>();
    final private static String INSERT_DETECTOR_GEOMETRY = " INSERT INTO detector_geometry_table( "
            + "   detector_geometry_name ) "
            + "  VALUES( ? );";
    
    final private static String SELECT_DETECTOR_GEOMETRY_BY_NAME = " SELECT detector_geometry_id, "
            + "   detector_geometry_name  "
            + " FROM detector_geometry_table AS t "
            + " WHERE t.detector_geometry_name <=> (?) ";
    
    public static final DetectorGeometryEntity load(final Connection conn, final Map<String, String> nameValuePairs) {
        DetectorGeometryEntity entity = DetectorGeometryEntity.find(conn, nameValuePairs);


        if (entity == null) {
            return null;
        }

        // found entity in cache, so return it...
        if (entity.getGeometryId() != null) {
            return entity;
        } 
        
        try (final PreparedStatement statement = conn.prepareStatement(DetectorGeometryEntity.INSERT_DETECTOR_GEOMETRY,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getGeometryName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getGeometryName());
            }
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    entity.setGeometryId(key);
                    DetectorGeometryEntity.cache.put(entity.getGeometryName(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Inserting detector material from pairs: " + nameValuePairs, e);
        }
        return null;
    }
    
    
    public static final DetectorGeometryEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final DetectorGeometryEntity entity = new DetectorGeometryEntity();
        String detectorGeometryName = nameValuePairs.get(DetectorGeometryEntity.NAME_VALUE_PAIR_KEY);
        if (detectorGeometryName == null) {
            // return null;
            // IAEA has asked that entity's with single column have
            // null entered in the column so that relationships
            // will still have an valid detectory geometry id
            // to point to. The other option would be to
            // not insert detector geometries that don't have
            // a name. This can be achieved by returning null
            // as s commented above.
            entity.setGeometryName(null);
        } else {
            entity.setGeometryName(detectorGeometryName.trim());
        }

        for (final DetectorGeometryEntity cachedEntity : DetectorGeometryEntity.cache.values()) {
            if (DetectorGeometryEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        // now check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(DetectorGeometryEntity.SELECT_DETECTOR_GEOMETRY_BY_NAME)) {
            if (entity.getGeometryName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getGeometryName());
            }
            statement.setString(1, entity.getGeometryName());
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setGeometryId(rs.getInt("detector_geometry_id"));
                    
                    DetectorGeometryEntity.cache.put(entity.getGeometryName(), entity);
                    return entity;
                } 
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Analyzer for name: " + entity, e);
        }
        return entity;
    } 
    
    /**
     * Returns true if all non-id fields in the first DetectorMateriealEntity are equal to all corresponding non-id fields in the second DetectorGeometryEntity fields. Don't use the id fields because those are not available until an entity has been saved to the database.
     * 
     * @param first
     * @param second
     * @return
     */
    public static boolean haveEqualValues(final DetectorGeometryEntity first,
            final DetectorGeometryEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getGeometryName() == null) {
            if (second.getGeometryName() != null) {
                return false;
            }
        } else if (!first.getGeometryName().equals(second.getGeometryName())) {
            return false;
        }

        return true;
    }
}
