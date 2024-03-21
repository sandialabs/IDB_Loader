package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SampleChemicalCompositionEntity {
    private Integer sampleChemicalCompositionId;
    private String sampleChemicalCompositionName;
//    private MeasurementNoteEntity noteEntity;
    private Set<MeasurementNoteEntity> notes;
    
    public Integer getSampleChemicalCompositionId() {
        return sampleChemicalCompositionId;
    }

    public void setSampleChemicalCompositionId(Integer sampleChemicalCompositionId) {
        this.sampleChemicalCompositionId = sampleChemicalCompositionId;
    }

    public String getSampleChemicalCompositionName() {
        return sampleChemicalCompositionName;
    }

    public void setSampleChemicalCompositionName(String sampleChemicalCompositionName) {
        this.sampleChemicalCompositionName = sampleChemicalCompositionName;
    }

    
    public Set<MeasurementNoteEntity> getNotes() {
        return notes;
    }

    public void setNotes(Set<MeasurementNoteEntity> notes) {
        this.notes = notes;
    }
//    
//    public MeasurementNoteEntity getNoteEntity() {
//        return noteEntity;
//    }
//
//    public void setNoteEntity(MeasurementNoteEntity note) {
//        this.noteEntity = note;
//    }

    @Override
    public String toString() {
        return "SampleChemicalCompositioin [sampleChemcialCompositionId=" + sampleChemicalCompositionId
                + ", sampleChemicalCompositionName=" + sampleChemicalCompositionName
                +  "]";
    }

    final private static String NAME_VALUE_PAIR_KEY = "sample.material.chemical_composition";
    final private static Map<String, SampleChemicalCompositionEntity> cache = new HashMap<>();
    final private static String INSERT_SOURCE_MATERIAL = " INSERT INTO sample_chemical_composition_table( "
            + "   sample_chemical_composition_name ) "
            + "  VALUES( ? );";
    
    final private static String SELECT_SOURCE_CHEMICAL_COMPOSITION_BY_NAME = " SELECT sample_chemical_composition_id, "
            + "   sample_chemical_composition_name "
            + " FROM sample_chemical_composition_table AS s "
            + " WHERE s.sample_chemical_composition_name <=> (?) ";

    public static final SampleChemicalCompositionEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final SampleChemicalCompositionEntity entity = SampleChemicalCompositionEntity.find(conn, nameValuePairs);

        // normally through an exception if can't find or make a non-null entity,
        // but chemical composition table has just one column, so if it
        // is null the entity will be null. Just return null.
        if (entity == null) {
            return null;
        }
        if (entity.getSampleChemicalCompositionId() != null) {
            return entity;
        }
        
        try (final PreparedStatement statement = conn.prepareStatement(
                SampleChemicalCompositionEntity.INSERT_SOURCE_MATERIAL,
                Statement.RETURN_GENERATED_KEYS)) {

            if (entity.getSampleChemicalCompositionName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getSampleChemicalCompositionName());
            }
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSampleChemicalCompositionId(key);
                    SampleChemicalCompositionEntity.cache.put(entity.getSampleChemicalCompositionName(),
                            entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting sample material form from pairs: " + nameValuePairs, e);
        }
        return null;
    }


    private static SampleChemicalCompositionEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

                final SampleChemicalCompositionEntity entity = new SampleChemicalCompositionEntity();
        String sampleChemicalCompositionName = nameValuePairs
                .get(SampleChemicalCompositionEntity.NAME_VALUE_PAIR_KEY);
        if (sampleChemicalCompositionName == null) {
            // return null;
            entity.setSampleChemicalCompositionName(null);
        }  else {
            entity.setSampleChemicalCompositionName(sampleChemicalCompositionName.trim());
        }

        
        // See if we already have created this type and if so just return it..
        for (final SampleChemicalCompositionEntity cachedEntity : SampleChemicalCompositionEntity.cache.values()) {
            if (SampleChemicalCompositionEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        try (final PreparedStatement statement = conn
                .prepareStatement(SampleChemicalCompositionEntity.SELECT_SOURCE_CHEMICAL_COMPOSITION_BY_NAME)) {


            if (entity.getSampleChemicalCompositionName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getSampleChemicalCompositionName());
            }
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    final SampleChemicalCompositionEntity retrievedEntity = new SampleChemicalCompositionEntity();
                    retrievedEntity.setSampleChemicalCompositionId(rs.getInt("sample_chemical_composition_id"));
                    retrievedEntity.setSampleChemicalCompositionName(rs.getString("sample_chemical_composition_name"));
                    
                    // Don't need to set notes because they shouldn't be the same between
                    // database loads
                    SampleChemicalCompositionEntity.cache.put(retrievedEntity.getSampleChemicalCompositionName(), retrievedEntity);
                    return retrievedEntity;
                } 
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Sample Material Form for name: " + entity, e);
        }
        // for sample material type, also need to check database...
        return entity;
    }
    
    public static boolean haveEqualValues(final SampleChemicalCompositionEntity first,
            final SampleChemicalCompositionEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }
        
        if (first.getSampleChemicalCompositionName()== null) {
            if (second.getSampleChemicalCompositionName() != null) {
                return false;
            }
        } else if (!first.getSampleChemicalCompositionName().equals(second.getSampleChemicalCompositionName())) {
            return false;
        }
        
        return true;
    }
}
