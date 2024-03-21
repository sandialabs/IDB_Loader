package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MissingDataNoteEntity {

    private Integer missingDataNoteId;
    private LoadHistoryEntity loadHistoryEntity;
    private Integer missingDataNoteFlagValue;
    private String missingDataNoteDescription;

    public Integer getMissingDataNoteId() {
        return missingDataNoteId;
    }

    public void setMissingDataNoteId(final Integer missingDataNoteId) {
        this.missingDataNoteId = missingDataNoteId;
    }

    public LoadHistoryEntity getLoadHistoryEntity() {
        return loadHistoryEntity;
    }

    public void setLoadHistoryEntity(LoadHistoryEntity loadHistoryEntity) {
        this.loadHistoryEntity = loadHistoryEntity;
    }

    public Integer getMissingDataNoteFlagValue() {
        return missingDataNoteFlagValue;
    }

    public void setMissingDataNoteFlagValue(Integer missingDataNoteFlagValue) {
        this.missingDataNoteFlagValue = missingDataNoteFlagValue;
    }

    public String getMissingDataNoteDescription() {
        return missingDataNoteDescription;
    }

    public void setMissingDataNoteDescription(String missingDataNoteDescription) {
        this.missingDataNoteDescription = missingDataNoteDescription;
    }

    final private static Set<MissingDataNoteEntity> cache = new HashSet<>();
    final private static String NAME_VALUE_PAIR_MISSING_DATA_NOTE_FLAG_VALUE = "missing.data.note.flag.value";
    final private static String NAME_VALUE_PAIR_MISSING_DATA_NOTE_DESCRIPTION = "missing.data.note.description";

    final private static String INSERT_NEW_NOTE = " INSERT INTO missing_data_note_table("
            + " load_history_id, missing_data_note_flag_value, missing_data_note_description) "
            + "  VALUES( ?,?,? );";
    final private static String SELECT_NOTE = " SELECT missing_data_note_id, load_history_id,  "
            + "    missing_data_note_flag_value, missing_data_note_description "
            + "  FROM missing_data_note_table AS n "
            + "  WHERE n.load_history_id = (?) "
            + "    AND n.missing_data_note_flag_value = (?);";

    static final public MissingDataNoteEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        MissingDataNoteEntity entity = MissingDataNoteEntity.find(conn, nameValuePairs);

        if (entity == null) {
            return null;
        }
        
        // found entity, so return it...
        if (entity.getMissingDataNoteId() != null) {
            return entity;
        }

        try (final PreparedStatement statement = conn.prepareStatement(
                MissingDataNoteEntity.INSERT_NEW_NOTE,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, entity.getLoadHistoryEntity().getLoadHistoryId());
            statement.setInt(2, entity.getMissingDataNoteFlagValue());
            statement.setString(3, entity.getMissingDataNoteDescription());

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }

                    entity.setMissingDataNoteId(key);
                    ;
                    MissingDataNoteEntity.cache.add(entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting missing note data from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    public static final MissingDataNoteEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final MissingDataNoteEntity entity = new MissingDataNoteEntity();
        entity.setLoadHistoryEntity(LoadHistoryEntity.load(conn, nameValuePairs));

        final String noteFlagValueString = nameValuePairs.get(MissingDataNoteEntity.NAME_VALUE_PAIR_MISSING_DATA_NOTE_FLAG_VALUE);
        if (noteFlagValueString == null) {
            return null;
        }
        entity.setMissingDataNoteFlagValue(Integer.valueOf(noteFlagValueString.trim()));

        final String noteDescription = nameValuePairs.get(MissingDataNoteEntity.NAME_VALUE_PAIR_MISSING_DATA_NOTE_DESCRIPTION);
        if (noteDescription == null) {
            return null;
        }
        entity.setMissingDataNoteDescription(noteDescription);

        for (final MissingDataNoteEntity cachedEntity : MissingDataNoteEntity.cache) {
            if (MissingDataNoteEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // now check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(MissingDataNoteEntity.SELECT_NOTE)) {

            statement.setInt(1, entity.getLoadHistoryEntity().getLoadHistoryId());
            statement.setInt(2, entity.getMissingDataNoteFlagValue());

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setMissingDataNoteId(rs.getInt("missing_data_note_id"));

                    MissingDataNoteEntity.cache.add(entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Inserting missing data notes: " + nameValuePairs, e);
        }

        return entity;
    }

    public static boolean haveEqualValues(final MissingDataNoteEntity first, final MissingDataNoteEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getLoadHistoryEntity() == null) {
            if (second.getLoadHistoryEntity() != null) {
                return false;
            }
        } else if (!LoadHistoryEntity.haveEqualValues(first.getLoadHistoryEntity(), second.getLoadHistoryEntity())) {
            return false;
        }

        if (first.getMissingDataNoteFlagValue() == null) {
            if (second.getMissingDataNoteFlagValue() != null) {
                return false;
            }
        } else if (!first.getMissingDataNoteFlagValue().equals(second.getMissingDataNoteFlagValue())) {
            return false;
        }

        if (first.getMissingDataNoteDescription() == null) {
            if (second.getMissingDataNoteDescription() != null) {
                return false;
            }
        } else if (!first.getMissingDataNoteDescription().equals(second.getMissingDataNoteDescription())) {
            return false;
        }

        return true;
    }
}
