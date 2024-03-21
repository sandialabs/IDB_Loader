package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DetectorTypeEntity {
    private Integer detectorTypeId;
    private DetectorMaterialEntity detectorMaterialEntity;
    private DetectorGeometryEntity detectorGeometryEntity;

    public DetectorTypeEntity() {
        
    }
    public DetectorTypeEntity(DetectorMaterialEntity detectorMaterialEntity,
            DetectorGeometryEntity detectorGeometryEntity) {
        super();
        this.detectorMaterialEntity = detectorMaterialEntity;
        this.detectorGeometryEntity = detectorGeometryEntity;
    }

    public DetectorTypeEntity(Integer detectorTypeId, DetectorMaterialEntity detectorMaterialEntity,
            DetectorGeometryEntity detectorGeometryEntity) {
        super();
        this.detectorTypeId = detectorTypeId;
        this.detectorMaterialEntity = detectorMaterialEntity;
        this.detectorGeometryEntity = detectorGeometryEntity;
    }

    public Integer getDetectorTypeId() {
        return detectorTypeId;
    }

    public void setDetectorTypeId(Integer detectorTypeId) {
        this.detectorTypeId = detectorTypeId;
    }

    public DetectorMaterialEntity getDetectorMaterialEntity() {
        return detectorMaterialEntity;
    }

    public void setDetectorMaterialEntity(DetectorMaterialEntity detectorMaterialEntity) {
        this.detectorMaterialEntity = detectorMaterialEntity;
    }

    public DetectorGeometryEntity getDetectorGeometryEntity() {
        return detectorGeometryEntity;
    }

    public void setDetectorGeometryEntity(DetectorGeometryEntity detectorGeometryEntity) {
        this.detectorGeometryEntity = detectorGeometryEntity;
    }

    @Override
    public String toString() {
        return "DetectorTypeEntity [detectorTypeId=" + detectorTypeId + ", detectorMaterialEntity="
                + detectorMaterialEntity + ", detectorGeometryEntity=" + detectorGeometryEntity + "]";
    }

    final static private Set<DetectorTypeEntity> cache = new HashSet<>();

    final private static String INSERT_DETECTOR_TYPE = " INSERT INTO detector_type_table( "
            + "   detector_material_id, detector_geometry_id )"
            + "  VALUES( ?,? );";
    
    final private static String SELECT_DETECTOR_TYPE_BY_MATERIAL_GEOMETRY = " SELECT detector_type_id, "
            + "   detector_material_id, detector_geometry_id  "
            + " FROM detector_type_table AS t "
            + " WHERE t.detector_material_id = (?) "
            + "  AND  t.detector_geometry_id = (?); ";

    static final public DetectorTypeEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        DetectorTypeEntity entity = DetectorTypeEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make detector_type: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getDetectorTypeId() != null) {
            return entity;
        } 

        try (final PreparedStatement statement = conn.prepareStatement(
                DetectorTypeEntity.INSERT_DETECTOR_TYPE,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getDetectorMaterialEntity() == null) {
                statement.setNull(1, Types.INTEGER);
            } else {
                statement.setInt(1, entity.getDetectorMaterialEntity().getMaterialId());
            }

            if (entity.getDetectorGeometryEntity() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, entity.getDetectorGeometryEntity().getGeometryId());
            }
            ;
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }

                    entity.setDetectorTypeId(key);
                    DetectorTypeEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting sample material form from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    public static final DetectorTypeEntity find(final Connection conn, final Map<String, String> nameValuePairs) {
        
        final DetectorTypeEntity entity = new DetectorTypeEntity();
        entity.setDetectorMaterialEntity(DetectorMaterialEntity.load(conn, nameValuePairs));
        entity.setDetectorGeometryEntity(DetectorGeometryEntity.load(conn, nameValuePairs));
        
        for (final DetectorTypeEntity cachedEntity : DetectorTypeEntity.cache) {
            if (DetectorTypeEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        // now check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(DetectorTypeEntity.SELECT_DETECTOR_TYPE_BY_MATERIAL_GEOMETRY)) {
            if (entity.getDetectorMaterialEntity() == null) {
                statement.setNull(1, Types.INTEGER);
            } else {
                statement.setInt(1, entity.getDetectorMaterialEntity().getMaterialId());
            }
            if (entity.getDetectorGeometryEntity() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, entity.getDetectorGeometryEntity().getGeometryId());
            }

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setDetectorTypeId(rs.getInt("detector_type_id"));
                    
                    DetectorTypeEntity.cache.add(entity);
                    return entity;
                } 
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Analyzer for name: " + entity, e);
        }
        
        return entity;
    }

    /**
     * Returns true if all non-id fields in the first DetectorMateriealEntity are equal to all corresponding non-id fields in the second DetectorMaterialEntity fields. Don't use the id fields because those are not available until an entity has been saved to the database.
     * 
     * @param first
     * @param second
     * @return
     */
    public static boolean haveEqualValues(final DetectorTypeEntity first,
            final DetectorTypeEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
                return false;
            
        }

        if (!DetectorMaterialEntity.haveEqualValues(first.getDetectorMaterialEntity(),
                second.getDetectorMaterialEntity())) {
            return false;
        }

        if (!DetectorGeometryEntity.haveEqualValues(first.getDetectorGeometryEntity(),
                second.getDetectorGeometryEntity())) {
            return false;
        }
        return true;
    }
}
