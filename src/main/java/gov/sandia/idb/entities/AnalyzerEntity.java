package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AnalyzerEntity {

    private Integer analyzerId;
    private String analyzerName;
    public Integer getAnalyzerId() {
        return analyzerId;
    }
    public void setAnalyzerId(Integer analyzerId) {
        this.analyzerId = analyzerId;
    }
    public String getAnalyzerName() {
        return analyzerName;
    }
    public void setAnalyzerName(String analyzerName) {
        this.analyzerName = analyzerName;
    }
    
    @Override
    public String toString() {
        return "AnalyzerEntity [analyzerId=" + analyzerId + ", analyzerName=" + analyzerName + "]";
    }

    final static private String ANALYZER_NAME_NAME_VALUE_PAIRS = "configuration.detector.analyzer";
    final static private Set<AnalyzerEntity> cache = new HashSet<>();

    final private static String INSERT_ANALYZER = " INSERT INTO analyzer_table( "
            + "   analyzer_name )"
            + "  VALUES( ? );";
    
    final private static String SELECT_ANALYZER_BY_NAME = " SELECT analyzer_id, "
            + "   analyzer_name  "
            + " FROM analyzer_table AS t "
            + " WHERE t.analyzer_name <=> (?) ";
    
    static final public AnalyzerEntity load(final Connection conn,
                                            final Map<String, String> nameValuePairs) {


        final AnalyzerEntity entity = find(conn, nameValuePairs);

        // analyzer only has name field and it can be unknown or
        // null. We don't want to add either to database, so
        // it is possible the returned entity will be null. Don't
        // throw the normal can't find exception, instead just 
        // return null so nothing gets inserted.
        if (entity == null) {
            // throw new RuntimeException("Analyzer entity is null " + nameValuePairs);
            return null;
        }


        if (entity.getAnalyzerId() != null) {
            return entity;
        }
 
        // found new entity, load it...
        try (final PreparedStatement statement = conn.prepareStatement(
                AnalyzerEntity.INSERT_ANALYZER,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getAnalyzerName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getAnalyzerName());
            }
;
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }

                    entity.setAnalyzerId(key);;
                    AnalyzerEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting analyzer entity: " + nameValuePairs, e);
        }
        
        return null;
    }
    
    public static final AnalyzerEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final AnalyzerEntity entity = new AnalyzerEntity();
        final String analyzerName = nameValuePairs.get(AnalyzerEntity.ANALYZER_NAME_NAME_VALUE_PAIRS);
        // if name is null, then return null because the only
        // field in analzyer is name.
        if (analyzerName == null) {
            entity.setAnalyzerName(null);
        } else {
            entity.setAnalyzerName(analyzerName.trim());
        }

        
        for (final AnalyzerEntity cachedEntity : AnalyzerEntity.cache) {
            if (AnalyzerEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        final String preparedStatment = AnalyzerEntity.SELECT_ANALYZER_BY_NAME;
        try (final PreparedStatement statement = conn
                .prepareStatement(preparedStatment)) {
            if (entity.getAnalyzerName() != null) {
                statement.setString(1, entity.getAnalyzerName());
            } else {
                statement.setString(1,entity.getAnalyzerName());
            }

            try (final ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    final AnalyzerEntity retrievedEntity = new AnalyzerEntity();
                    retrievedEntity.setAnalyzerId(rs.getInt("analyzer_id"));
                    retrievedEntity.setAnalyzerName(rs.getString("analyzer_name"));
                    
                    AnalyzerEntity.cache.add(retrievedEntity);
                    return retrievedEntity;
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
    public static boolean haveEqualValues(final AnalyzerEntity first,
            final AnalyzerEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getAnalyzerName() == null) {
            if (second.getAnalyzerName() != null) {
                return false;
            }
        } else if (!first.getAnalyzerName().equalsIgnoreCase(second.getAnalyzerName())) {
            return false;
        }

        return true;
    }
}
