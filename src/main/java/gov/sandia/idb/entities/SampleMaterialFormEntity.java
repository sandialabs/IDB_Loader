package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class SampleMaterialFormEntity {
    private Integer sampleMaterialFormId;
    private String sampleMaterialFormName;
    private MeasurementNoteEntity noteEntity;
//    private Set<MeasurementNoteEntity> notes;

    public Integer getSampleMaterialFormId() {
        return sampleMaterialFormId;
    }

    public void setSamapleMaterialFormId(Integer samapleMaterialFormId) {
        this.sampleMaterialFormId = samapleMaterialFormId;
    }

    public String getSampleMaterialFormName() {
        return sampleMaterialFormName;
    }


    public void setSampleMaterialFormId(Integer sampleMaterialFormId) {
        this.sampleMaterialFormId = sampleMaterialFormId;
    }

    public void setSampleMaterialFormName(String sampleMaterialFormName) {
        this.sampleMaterialFormName = sampleMaterialFormName;
    }
//  
//    public Set<MeasurementNoteEntity> getNotes() {
//        return notes;
//    }
//
//    public void setNotes(Set<MeasurementNoteEntity> notes) {
//        this.notes = notes;
//    }
    
    public MeasurementNoteEntity getNoteEntity() {
        return noteEntity;
    }

    public void setNoteEntity(MeasurementNoteEntity note) {
        this.noteEntity = note;
    }
    @Override
    public String toString() {
        return "SampleMaterialFormEntity [samapleMaterialFormId=" + sampleMaterialFormId + ", sampleMaterialFormName="
                + sampleMaterialFormName + ", note=" + noteEntity + "]";
    }

    final private static String NAME_VALUE_PAIR_KEY = "sample.material.form";
    final private static Map<String, SampleMaterialFormEntity> cache = new HashMap<>();
    final private static String INSERT_SOURCE_MATERIAL = " INSERT INTO sample_material_form_table( "
            + "   sample_material_form_name ) "
            + "  VALUES( ? );";
    
    final private static String SELECT_SAMPLE_MATERIAL_FORM_BY_NAME = " SELECT sample_material_form_id, "
            + "   sample_material_form_name "
            + " FROM sample_material_form_table AS s "
            + " WHERE s.sample_material_form_name <=> (?) ";

    public static final SampleMaterialFormEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        final SampleMaterialFormEntity entity = SampleMaterialFormEntity.find(conn, nameValuePairs);
        if (entity == null) {
            // for sample material form, only field is name. If that is null or unknown,
            // then the material form row should be set to null and don't throw the
            // normal exception about not having enough information...
            return null;
        }
        if (entity.getSampleMaterialFormId() != null) {
            return entity;
        }
        

        try (final PreparedStatement statement = conn.prepareStatement(SampleMaterialFormEntity.INSERT_SOURCE_MATERIAL,
                Statement.RETURN_GENERATED_KEYS)) {


            if (entity.getSampleMaterialFormName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getSampleMaterialFormName());
            }
            
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSamapleMaterialFormId(key);
                    SampleMaterialFormEntity.cache.put(entity.getSampleMaterialFormName(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting sample material form from pairs: " + nameValuePairs, e);
        }
        return null;
    }
    
    private static SampleMaterialFormEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final SampleMaterialFormEntity entity = new SampleMaterialFormEntity();
        String sampleMaterialFormName = nameValuePairs.get(SampleMaterialFormEntity.NAME_VALUE_PAIR_KEY);
        if (sampleMaterialFormName == null) {
            // return null;
            entity.setSampleMaterialFormName(null);
        } else {
            entity.setSampleMaterialFormName(sampleMaterialFormName.trim());
        }

        // See if we already have created this type and if so just return it..
        for (final SampleMaterialFormEntity cachedEntity : SampleMaterialFormEntity.cache.values()) {
            if (SampleMaterialFormEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }
        
        try (final PreparedStatement statement = conn
                .prepareStatement(SampleMaterialFormEntity.SELECT_SAMPLE_MATERIAL_FORM_BY_NAME)) {


            if (entity.getSampleMaterialFormName() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, entity.getSampleMaterialFormName());
            }
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    final SampleMaterialFormEntity retrievedEntity = new SampleMaterialFormEntity();
                    retrievedEntity.setSampleMaterialFormId(rs.getInt("sample_material_form_id"));
                    retrievedEntity.setSampleMaterialFormName(rs.getString("sample_material_form_id"));
                    
                    // If there are notes, they are unique for each measurement, so don't retrieve other
                    // measurement's notes...
                    SampleMaterialFormEntity.cache.put(retrievedEntity.getSampleMaterialFormName(), retrievedEntity);
                    return retrievedEntity;
                } 
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Sample Material Form for name: " + sampleMaterialFormName, e);
        }
        // for sample material type, also need to check database...
        return entity;
    }
    
    public static boolean haveEqualValues(final SampleMaterialFormEntity first,
            final SampleMaterialFormEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }
        
        if (first.getSampleMaterialFormName()== null) {
            if (second.getSampleMaterialFormName() != null) {
                return false;
            }
        } else if (!first.getSampleMaterialFormName().equals(second.getSampleMaterialFormName())) {
            return false;
        }
        
        return true;
    }

}
