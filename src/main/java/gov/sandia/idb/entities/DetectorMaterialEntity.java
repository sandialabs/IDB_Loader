package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class DetectorMaterialEntity {
    private Integer materialId;
    private String materialName;

    public DetectorMaterialEntity() {
        
    }
    
    public DetectorMaterialEntity(String materialName) {
        super();
        this.materialName = materialName;
    }

    public DetectorMaterialEntity(Integer materialTypeId, String materialName) {
        super();
        this.materialId = materialTypeId;
        this.materialName = materialName;
    }

    public Integer getMaterialId() {
        return materialId;
    }

    public void setMaterialId(Integer materialId) {
        this.materialId = materialId;
    }

    public String getMaterialName() {
        return materialName;
    }

    public void setMaterialName(String materialName) {
        this.materialName = materialName;
    }

    @Override
    public String toString() {
        return "DetectorMaterialEntity [materialId=" + materialId + ", materialName=" + materialName
                + "]";
    }

    final private static String NAME_VALUE_PAIR_KEY = "configuration.detector.material";
    final private static Map<String, DetectorMaterialEntity> cache = new HashMap<>();
    final private static String INSERT_DETECTOR_MATERIAL = " INSERT INTO detector_material_table( "
            + "   detector_material_name ) "
            + "  VALUES( ? );";
    
    final private static String SELECT_DETECTOR_MATERIAL_TYPE_BY_NAME = " SELECT detector_material_id, "
            + "   detector_material_name  "
            + " FROM detector_material_table AS t "
            + " WHERE t.detector_material_name <=> (?) ";

    public static final DetectorMaterialEntity load(final Connection conn, final Map<String, String> nameValuePairs) {
        DetectorMaterialEntity entity = DetectorMaterialEntity.find(conn, nameValuePairs);

        // For single data column entities, want a null
        // value in cases where there isn't any data
        if (entity == null) {
            return null;
        }

        // found entity in cache, so return it...
        if (entity.getMaterialId() != null) {
            return entity;
        } 

        try (final PreparedStatement statement = conn.prepareStatement(DetectorMaterialEntity.INSERT_DETECTOR_MATERIAL,
                Statement.RETURN_GENERATED_KEYS)) {
                    
            if (entity.getMaterialName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getMaterialName());
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    entity.setMaterialId(key);
                    DetectorMaterialEntity.cache.put(entity.getMaterialName(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Inserting detector material from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    
    public static final DetectorMaterialEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final DetectorMaterialEntity entity = new DetectorMaterialEntity();
        String detectorMaterialName = nameValuePairs.get(DetectorMaterialEntity.NAME_VALUE_PAIR_KEY);
        if (detectorMaterialName == null) {
            // return null;
            entity.setMaterialName(null);
        } else {
            entity.setMaterialName(detectorMaterialName.trim());
        }

        for (final DetectorMaterialEntity cachedEntity : DetectorMaterialEntity.cache.values()) {
            if (DetectorMaterialEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        // now check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(DetectorMaterialEntity.SELECT_DETECTOR_MATERIAL_TYPE_BY_NAME)) {

            if (entity.getMaterialName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getMaterialName());
            }
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setMaterialId(rs.getInt("detector_material_id"));
                    DetectorMaterialEntity.cache.put(entity.getMaterialName(), entity);
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
    public static boolean haveEqualValues(final DetectorMaterialEntity first,
            final DetectorMaterialEntity second) {
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

        if (first.getMaterialName() == null) {
            if (second.getMaterialName() != null) {
                return false;
            }
        } else if (!first.getMaterialName().equals(second.getMaterialName())) {
            return false;
        }

        return true;
    }

}
