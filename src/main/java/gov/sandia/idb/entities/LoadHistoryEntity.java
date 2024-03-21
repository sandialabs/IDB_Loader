package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class LoadHistoryEntity {

    private Integer loadHistoryId;
    private String dataProvider;
    private String laboratory;
    private String contact_email;
    private Instant loadHistoryTimestamp;
    private String loadHistoryDescription;

    public Integer getLoadHistoryId() {
        return loadHistoryId;
    }

    public void setLoadHistoryId(Integer loadHistoryId) {
        this.loadHistoryId = loadHistoryId;
    }

    public Instant getLoadHistoryTimestamp() {
        return loadHistoryTimestamp;
    }

    public void setLoadHistoryTimestamp(Instant loadHistoryTimestamp) {
        this.loadHistoryTimestamp = loadHistoryTimestamp;
    }

    public String getLoadHistoryDescription() {
        return loadHistoryDescription;
    }

    public void setLoadHistoryDescription(String loadHistoryDescription) {
        this.loadHistoryDescription = loadHistoryDescription;
    }

    public String getDataProvider() {
        return dataProvider;
    }

    public void setDataProvider(String dataProvider) {
        this.dataProvider = dataProvider;
    }

    public String getLaboratory() {
        return laboratory;
    }

    public void setLaboratory(String laboratory) {
        this.laboratory = laboratory;
    }

    public String getContact_email() {
        return contact_email;
    }

    public void setContact_email(String contact_email) {
        this.contact_email = contact_email;
    }

    @Override
    public String toString() {
        return "LoadHistoryEntity [loadHistoryId=" + loadHistoryId + ", dataProvider=" + dataProvider + ", laboratory="
                + laboratory + ", contact_email=" + contact_email + ", loadHistoryTimestamp=" + loadHistoryTimestamp
                + ", loadHistoryDescription=" + loadHistoryDescription + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((loadHistoryId == null) ? 0 : loadHistoryId.hashCode());
        result = prime * result + ((dataProvider == null) ? 0 : dataProvider.hashCode());
        result = prime * result + ((laboratory == null) ? 0 : laboratory.hashCode());
        result = prime * result + ((contact_email == null) ? 0 : contact_email.hashCode());
        result = prime * result + ((loadHistoryTimestamp == null) ? 0 : loadHistoryTimestamp.hashCode());
        result = prime * result + ((loadHistoryDescription == null) ? 0 : loadHistoryDescription.hashCode());
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
        LoadHistoryEntity other = (LoadHistoryEntity) obj;
        if (loadHistoryId == null) {
            if (other.loadHistoryId != null)
                return false;
        } else if (!loadHistoryId.equals(other.loadHistoryId))
            return false;
        if (dataProvider == null) {
            if (other.dataProvider != null)
                return false;
        } else if (!dataProvider.equals(other.dataProvider))
            return false;
        if (laboratory == null) {
            if (other.laboratory != null)
                return false;
        } else if (!laboratory.equals(other.laboratory))
            return false;
        if (contact_email == null) {
            if (other.contact_email != null)
                return false;
        } else if (!contact_email.equals(other.contact_email))
            return false;
        if (loadHistoryTimestamp == null) {
            if (other.loadHistoryTimestamp != null)
                return false;
        } else if (!loadHistoryTimestamp.equals(other.loadHistoryTimestamp))
            return false;
        if (loadHistoryDescription == null) {
            if (other.loadHistoryDescription != null)
                return false;
        } else if (!loadHistoryDescription.equals(other.loadHistoryDescription))
            return false;
        return true;
    }

    final public static String NAME_VALUE_PAIR_LOAD_HISTORY_DESCRIPTION = "load.history.description";
    final public static String NAME_VALUE_DATA_PROVIDER = "data_provider";
    final public static String NAME_VALUE_PAIR_LABORATORY = "laboratory";
    final public static String NAME_VALUE_PAIR_CONTACT_EMAIL = "contact_email";
    final private static Map<Integer, LoadHistoryEntity> cache = new HashMap<>();
    final private static String INSERT_LOAD_HISTORY = " INSERT INTO load_history_table( "
            + "   load_history_description, data_provider, laboratory, contact_email ) "
            + "  VALUES( ?,?,?,? );";

    public static final LoadHistoryEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        final LoadHistoryEntity entity = LoadHistoryEntity.find(conn, nameValuePairs);

        if (entity == null) {
            throw new RuntimeException("Could not create or find a load history for " + nameValuePairs);
        }
        if (entity.getLoadHistoryId() != null) {
            return entity;
        }

        try (final PreparedStatement statement = conn.prepareStatement(LoadHistoryEntity.INSERT_LOAD_HISTORY,
                Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, entity.getLoadHistoryDescription());
            if (entity.getDataProvider() == null) {
                statement.setNull(2, Types.VARCHAR);
            } else {
                statement.setString(2, entity.getDataProvider());
            }
            if (entity.getLaboratory() == null) {
                statement.setNull(3, Types.VARCHAR);
            } else {
                statement.setString(3, entity.getLaboratory());
            }
            if (entity.getContact_email() == null) {
                statement.setNull(4, Types.VARCHAR);
            } else {
                statement.setString(4, entity.getContact_email());
            }
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setLoadHistoryId(key);
                    LoadHistoryEntity.cache.put(entity.getLoadHistoryId(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting load history entity: " + nameValuePairs, e);
        }

        return null;
    }

    private static final LoadHistoryEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String loadHistoryDescription = nameValuePairs
                .get(LoadHistoryEntity.NAME_VALUE_PAIR_LOAD_HISTORY_DESCRIPTION).trim();

        final LoadHistoryEntity entity = new LoadHistoryEntity();
        entity.setLoadHistoryDescription(loadHistoryDescription);
        entity.setDataProvider(nameValuePairs.get(LoadHistoryEntity.NAME_VALUE_DATA_PROVIDER));
        entity.setLaboratory(nameValuePairs.get(LoadHistoryEntity.NAME_VALUE_PAIR_LABORATORY));
        entity.setContact_email(nameValuePairs.get(LoadHistoryEntity.NAME_VALUE_PAIR_CONTACT_EMAIL));
        for (final LoadHistoryEntity cachedEntity : LoadHistoryEntity.cache.values()) {
            if (LoadHistoryEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // this entity has new values that need to be added to the
        // table, so return the non-cached entity...
        return entity;
    }

    public static boolean haveEqualValues(final LoadHistoryEntity first, final LoadHistoryEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getLoadHistoryDescription() == null) {
            if (second.getLoadHistoryDescription() != null) {
                return false;
            }
        } else if (!first.getLoadHistoryDescription().equals(second.getLoadHistoryDescription())) {
            return false;
        }

        if (first.getDataProvider() == null) {
            if (second.getDataProvider() != null) {
                return false;
            }
        } else if (!first.getDataProvider().equals(second.getDataProvider())) {
            return false;
        }

        if (first.getLaboratory() == null) {
            if (second.getLaboratory() != null) {
                return false;
            }
        } else if (!first.getLaboratory().equals(second.getLaboratory())) {
            return false;
        }

        if (first.getContact_email() == null) {
            if (second.getContact_email() != null) {
                return false;
            }
        } else if (!first.getContact_email().equals(second.getContact_email())) {
            return false;
        }

        return true;
    }
}
