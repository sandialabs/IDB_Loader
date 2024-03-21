package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DetectorEntity {
    private Integer detectorId;
    private DetectorTypeEntity detectorTypeEntity;
    private AnalyzerEntity analyzerEntity;

    public Integer getDetectorId() {
        return detectorId;
    }

    public void setDetectorId(Integer detectorId) {
        this.detectorId = detectorId;
    }

    public DetectorTypeEntity getDetectorTypeEntity() {
        return detectorTypeEntity;
    }

    public void setDetectorTypeEntity(DetectorTypeEntity detectorTypeEntity) {
        this.detectorTypeEntity = detectorTypeEntity;
    }

    public AnalyzerEntity getAnalzyerEntity() {
        return analyzerEntity;
    }

    public void setAnalzyerEntity(final AnalyzerEntity analyzerEntity) {
        this.analyzerEntity = analyzerEntity;
    }

    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((detectorId == null) ? 0 : detectorId.hashCode());
        result = prime * result + ((detectorTypeEntity == null) ? 0 : detectorTypeEntity.hashCode());
        result = prime * result + ((analyzerEntity == null) ? 0 : analyzerEntity.hashCode());
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
        DetectorEntity other = (DetectorEntity) obj;
        if (detectorId == null) {
            if (other.detectorId != null)
                return false;
        } 

        // if the id is true, then they are the same
        if (detectorId.equals(other.getDetectorId())) {
            return true;
        }
        if (detectorTypeEntity == null) {
            if (other.detectorTypeEntity != null)
                return false;
        } else if (!detectorTypeEntity.equals(other.detectorTypeEntity))
            return false;
        if (analyzerEntity == null) {
            if (other.analyzerEntity != null)
                return false;
        } else if (!analyzerEntity.equals(other.analyzerEntity))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DetectorEntity [detectorId=" + detectorId + ", detectorTypeEntity=" + detectorTypeEntity
                + ", analzyerEntity=" + analyzerEntity + "]";
    }

    final static private Set<DetectorEntity> cache = new HashSet<>();

    final private static String INSERT_DETECTOR = " INSERT INTO detector_table( "
            + "   detector_type_id, analyzer_id )"
            + "  VALUES( ?,? );";

    final private static String SELECT_DETECTOR_BY_DETECTOR_TYPE_ANALYZER = " SELECT detector_id, "
            + "   detector_type_id, analyzer_id  "
            + " FROM detector_table AS t "
            + " WHERE t.detector_type_id <=> (?) "
            + "   AND t.analyzer_id <=> (?); ";

    static final public DetectorEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        // if sample is in cache, then return the cached value...
        DetectorEntity entity = find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Unable to find or create detector..." + nameValuePairs);
        }

        if (entity.getDetectorId() != null) {
            return entity;
        }

        // this is new detector information, so save it...
        try (final PreparedStatement statement = conn.prepareStatement(
                DetectorEntity.INSERT_DETECTOR,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getDetectorTypeEntity() == null) {
                statement.setNull(1, Types.INTEGER);
            } else {
                statement.setInt(1, entity.getDetectorTypeEntity().getDetectorTypeId());
            }

            if (entity.getAnalzyerEntity() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, entity.getAnalzyerEntity().getAnalyzerId());
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }

                    entity.setDetectorId(key);
                    DetectorEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting sample material form from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    public static final DetectorEntity find(final Connection conn, final Map<String, String> nameValuePairs) {

        final DetectorEntity entity = new DetectorEntity();
        entity.setDetectorTypeEntity(DetectorTypeEntity.load(conn, nameValuePairs));
        entity.setAnalzyerEntity(AnalyzerEntity.load(conn, nameValuePairs));

        for (final DetectorEntity cachedEntity : DetectorEntity.cache) {
            if (DetectorEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // now check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(DetectorEntity.SELECT_DETECTOR_BY_DETECTOR_TYPE_ANALYZER)) {
            if (entity.getDetectorTypeEntity() == null) {
                statement.setNull(1, Types.INTEGER);
            } else {
                statement.setInt(1, entity.getDetectorTypeEntity().getDetectorTypeId());
            }
            if (entity.getAnalzyerEntity() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, entity.getAnalzyerEntity().getAnalyzerId());
            }

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setDetectorId(rs.getInt("detector_id"));

                    DetectorEntity.cache.add(entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Analyzer for name: " + entity, e);
        }

        return entity;
    }

    public static final boolean haveEqualValues(final DetectorEntity first,
            final DetectorEntity second) {
        // take care of null values first...
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;            
        }

        if (!AnalyzerEntity.haveEqualValues(first.getAnalzyerEntity(), second.getAnalzyerEntity())) {
            return false;
        }

        if (!DetectorTypeEntity.haveEqualValues(first.getDetectorTypeEntity(), second.getDetectorTypeEntity())) {
            return false;
        }

        return true;
    }
}
