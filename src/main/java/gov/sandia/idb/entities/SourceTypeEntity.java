package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class SourceTypeEntity {

    private Integer sourceTypeId;
    final private String sourceTypeName;
    final private String sourceTypeDescription;

    public SourceTypeEntity(String sourceTypeName, String sourceTypeDescription) {
        super();
        this.sourceTypeName = sourceTypeName;
        this.sourceTypeDescription = sourceTypeDescription;
    }

    public Integer getSourceTypeId() {
        return sourceTypeId;
    }

    public void setSourceTypeId(Integer sourceTypeId) {
        this.sourceTypeId = sourceTypeId;
    }

    public String getSourceTypeName() {
        return sourceTypeName;
    }

    public String getSourceTypeDescription() {
        return sourceTypeDescription;
    }

    @Override
    public String toString() {
        return "SourceTypeEntity [sourceTypeId=" + sourceTypeId + ", sourceTypeName=" + sourceTypeName
                + ", sourceTypeDescription=" + sourceTypeDescription + "]";
    }

    public static final String SOURCE_TYPE_NAME_NAME_VALUE_PAIR = "source.type.name";
    public static final String SOURCE_TYPE_DESCRIPTION_NAME_VALUE_PAIR = "source.type.description";

    final private static Map<String, SourceTypeEntity> cache = new HashMap<>();
    final private static String INSERT_SOURCE_TYPE = " INSERT INTO source_type_table( "
            + "   source_type_name, source_type_description ) "
            + "  VALUES( ?,? );";
    
    final private static String SELECT_SOURCE_TYPE_BY_NAME = " SELECT "
            + " source_type_id, source_type_name, source_type_description "
            + " FROM source_type_table "
            + " WHERE source_type_name = (?) ";

    public static final SourceTypeEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SourceTypeEntity entity = SourceTypeEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Not enough information to make source_type: " + nameValuePairs);
        }
        if (entity.getSourceTypeId() != null) {
            return entity;
        }
        
        // no database id, so need this entity to the db...
        try (final PreparedStatement statement = conn.prepareStatement(SourceTypeEntity.INSERT_SOURCE_TYPE,
                Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, entity.getSourceTypeName());
            if (entity.getSourceTypeDescription() == null) {
                statement.setNull(2, Types.VARCHAR);
            } else {
                statement.setString(2, entity.getSourceTypeDescription());
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSourceTypeId(key);
                }
            }
            SourceTypeEntity.cache.put(entity.getSourceTypeName(), entity);
            return entity;

        } catch (final Exception e) {
            throw new RuntimeException("Inserting source type entity: " + entity, e);
        }

    }

    public static SourceTypeEntity get(final String sourceTypeName) {
        return SourceTypeEntity.cache.get(sourceTypeName.toUpperCase());
    }
    
    
    private static final SourceTypeEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String sourceTypeName = nameValuePairs.get(SourceTypeEntity.SOURCE_TYPE_NAME_NAME_VALUE_PAIR);
        final String sourceTypeDescription = nameValuePairs.get(SourceTypeEntity.SOURCE_TYPE_DESCRIPTION_NAME_VALUE_PAIR);

        final SourceTypeEntity entity = new SourceTypeEntity(sourceTypeName, sourceTypeDescription);

        for (final SourceTypeEntity cachedEntity : SourceTypeEntity.cache.values()) {
            if (SourceTypeEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        // for Source Type also check the database to see if it is already
        // defined because the data isn't coming from a load file.
        try (final PreparedStatement statement = conn.prepareStatement(SourceTypeEntity.SELECT_SOURCE_TYPE_BY_NAME)) {
            statement.setString(1, entity.getSourceTypeName());
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setSourceTypeId(rs.getInt("source_type_id"));
                    SourceTypeEntity.cache.put(entity.getSourceTypeName(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Searching for source type: " + entity, e);
        }

        // if we get here, there isn't a source type with this name in 
        // the cache or the database, so need to create one...
        return entity;
    }
    public static boolean haveEqualValues(final SourceTypeEntity first, final SourceTypeEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getSourceTypeName()== null) {
            if (second.getSourceTypeName() != null) {
                return false;
            }
        } else if (!first.getSourceTypeName().equals(second.getSourceTypeName())) {
            return false;
        }

        if (first.getSourceTypeDescription() == null) {
            if (second.getSourceTypeDescription() != null) {
                return false;
            }
        } else if (!first.getSourceTypeDescription().equals(second.getSourceTypeDescription())) {
            return false;
        }

        return true;
    }
}
