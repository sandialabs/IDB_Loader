package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MeasurementEntity {

    private Integer measurementId;
    private Integer uidMetadata;
    private Integer uidSpectrum;
    private CertificateEntity certificateEntity;
    private SampleEntity sampleEntity;
    private ConfigurationEntity configurationEntity;
    private Set<AttenuatorEntity> attenuatorEntities;
    private SupplementaryMeasurementEntity supplementaryMeasurementEntity;

    public Integer getMeasurementId() {
        return measurementId;
    }

    public void setMeasurementId(Integer measurementId) {
        this.measurementId = measurementId;
    }

    public Integer getUidMetadata() {
        return uidMetadata;
    }

    public void setUidMetadata(Integer uidMetadata) {
        this.uidMetadata = uidMetadata;
    }

    public Integer getUidSpectrum() {
        return uidSpectrum;
    }

    public void setUidSpectrum(Integer uidSpectrum) {
        this.uidSpectrum = uidSpectrum;
    }

    public CertificateEntity getCertificateEntity() {
        return certificateEntity;
    }

    public void setCertificateEntity(CertificateEntity certificateEntity) {
        this.certificateEntity = certificateEntity;
    }

    public SampleEntity getSampleEntity() {
        return this.sampleEntity;
    }

    public void setSampleEntity(SampleEntity sampleEntity) {
        this.sampleEntity = sampleEntity;
    }

    public ConfigurationEntity getConfigurationEntity() {
        return configurationEntity;
    }

    public void setConfigurationEntity(ConfigurationEntity configurationEntity) {
        this.configurationEntity = configurationEntity;
    }

    public Set<AttenuatorEntity> getAttenuatorEntites() {
        return this.attenuatorEntities;
    }

    public void setAttenuatorEntities(Set<AttenuatorEntity> attenuatorEntities) {
        this.attenuatorEntities = attenuatorEntities;
    }
    
    public SupplementaryMeasurementEntity getSupplementaryMeasurementEntity() {
        return supplementaryMeasurementEntity;
    }

    public void setSupplementaryMeasurementEntity(SupplementaryMeasurementEntity supplementaryMeasurementEntity) {
        this.supplementaryMeasurementEntity = supplementaryMeasurementEntity;
    }

    @Override
    public String toString() {
        return "MeasurementEntity [measurementId=" + measurementId + ", uidMetadata=" + uidMetadata + ", uidSource="
                + uidSpectrum + ", certificateEntity=" + certificateEntity + ", sampleEntity=" + sampleEntity
                + ", configurationEntity=" + configurationEntity + ", supplementaryMeasurementEntity="
                + supplementaryMeasurementEntity + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((measurementId == null) ? 0 : measurementId.hashCode());
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
        MeasurementEntity other = (MeasurementEntity) obj;
        if (measurementId == null) {
            if (other.measurementId != null) {
                return false;
            } else {
                return MeasurementEntity.haveEqualValues(this, other);
            }
        } else if (!measurementId.equals(other.measurementId))
            return false;
        return true;
    }

    final static private String UID_SOURCE_NAME_VALUE_PAIR = "UID.source";
    final static private String UID_METADATA_NAME_VALUE_PAIR = "UID.metadata";

    final private static Map<Integer, MeasurementEntity> cache = new HashMap<>();
    
    final private static String INSERT_MEASUREMENT = " INSERT INTO measurement_table( "
            + "   uid_metadata, uid_source, certificate_id, sample_id, "
            + "   configuration_id, supplementary_measurement_id ) " + "  VALUES( ?,?,?,?,?, ? );";
    
    
    final private static String INSERT_ATTENUATOR_MEASUREMENT_JOIN = " INSERT INTO "
            + " attenuator_measurement_join_table( "
            + "   measurement_id, attenuator_id ) "
            + "  VALUES( ?,? );";
        

    public static final MeasurementEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        MeasurementEntity entity = MeasurementEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            return null;
        }


        // Found an entity in the cache, so return it (it will
        // already have its id because it has been saved to the db
        if (entity.getMeasurementId() != null) {
            boolean entityFound = true;
            if (entity.getAttenuatorEntites() != null) {
                for (final AttenuatorEntity attenuatorEntity : entity.getAttenuatorEntites()) {
                    if (attenuatorEntity.getAttenuatorId() == null) {
                        entityFound = false;
                        break;
                    }
                }
            }

            if (entityFound == true) {
                return entity;
            }
        }

        // need to create a new entity...
        try (final PreparedStatement statement = conn.prepareStatement(MeasurementEntity.INSERT_MEASUREMENT,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, entity.getUidMetadata());
            if (entity.getUidSpectrum() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, Integer.valueOf(entity.getUidSpectrum()));
            }

            if (entity.getCertificateEntity() == null) {
                statement.setNull(3, Types.INTEGER);
            } else {
                statement.setInt(3, entity.getCertificateEntity().getCertificateId());
            }

            if (entity.getSampleEntity() == null) {
                statement.setNull(4, Types.INTEGER);
            } else {
                statement.setInt(4, entity.getSampleEntity().getSampleId());
            }

            if (entity.getConfigurationEntity() == null) {
                statement.setNull(5, Types.INTEGER);
            } else {
                statement.setInt(5, entity.getConfigurationEntity().getConfigurationId());
            }

            if (entity.getSupplementaryMeasurementEntity() == null) {
                statement.setNull(6, Types.INTEGER);
            } else {
                statement.setInt(6, entity.getSupplementaryMeasurementEntity().getSupplementaryMeasurementId());
            }
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setMeasurementId(key);
                    
                    // Note that the cache is using the uidMetadata as a key
                    // rather than the database key because some sample entities
                    // need to reference back to the Measurement data and
                    // their input files have the uidMetadata.  
                    MeasurementEntity.cache.put(entity.getUidMetadata(), entity);
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting supplementary measurement values from pairs: " + nameValuePairs, e);
        }
        
        // Also need to insert values for the configuration--attenuator
        // join table. Do it here, because the join table isn't really
        // defined in the data that is being imported so the load(conn, namevaluepairs)
        // that is used for to load normal data isn't set up to create
        // a join table...
        if (entity.getAttenuatorEntites() == null || entity.getAttenuatorEntites().size() == 0) {
            return entity;
        }
        for (final AttenuatorEntity attenuatorEntity : entity.getAttenuatorEntites()) {
//            if (attenuatorEntity.getAttenuatorId() == null) {
                try (final PreparedStatement statement = conn.prepareStatement(
                        MeasurementEntity.INSERT_ATTENUATOR_MEASUREMENT_JOIN)) {
                    statement.setInt(1, entity.getMeasurementId());
                    statement.setInt(2, attenuatorEntity.getAttenuatorId());
                    statement.executeUpdate();
                } catch (final Exception e) {
                    throw new RuntimeException("Inserting attenuator-measurement join: " + entity);
                }
        }
        
        return null;
    }
    
    /**
     * The spectrum data needs a way to find the measurement using the uid metadata. 
     * The returned Measurement Entity should only come from the
     * measurement entity loaded with the spectrum csv file.
     * 
     * @param uidMetadata
     * @return
     */
    public static final  MeasurementEntity find(final Integer uidMetadata) {
        return MeasurementEntity.cache.get(uidMetadata);
    }
    

    /**
     * The measurement find does not check the database for duplicates because
     * the relationship to spectra is made via the UID.metadata field.
     * 
     * @param conn
     * @param nameValuePairs
     * @return
     */
    private static final MeasurementEntity find(final Connection conn, final Map<String, String> nameValuePairs) {

        final String uidMetadataString = nameValuePairs.get(MeasurementEntity.UID_METADATA_NAME_VALUE_PAIR);
        final String uidSourceString = nameValuePairs.get(MeasurementEntity.UID_SOURCE_NAME_VALUE_PAIR);

        if (uidMetadataString == null) {
            throw new RuntimeException("Metadata csv line without a UID.Metadata");
        }

        final MeasurementEntity entity = new MeasurementEntity();
        entity.setUidMetadata(Integer.valueOf(uidMetadataString));
        entity.setUidSpectrum(Integer.valueOf(uidSourceString));
        entity.setCertificateEntity(CertificateEntity.find(uidSourceString));
        entity.setConfigurationEntity(ConfigurationEntity.load(conn, nameValuePairs));
        entity.setSampleEntity(SampleEntity.load(conn, nameValuePairs));
        entity.setAttenuatorEntities(AttenuatorEntity.load(conn, nameValuePairs));
        entity.setSupplementaryMeasurementEntity(SupplementaryMeasurementEntity.load(conn, nameValuePairs));

        for (final MeasurementEntity cachedEntity : MeasurementEntity.cache.values()) {
            if (MeasurementEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // this measurement entity has new values that need to be added to the
        // measurement table, so return the non-cached entity. The measurement
        // entity does NOT check the database for possible duplicates because
        // data in the spectrum CSV files are related to the measurment CSV file
        // via the UID.metadata field. That field is unique within one set of 
        // CSV data, but not between sets. To make sure that the relationship
        // is valid, all measurement data is always loaded as is all spectrum 
        // data.
        return entity;
    }

    public static boolean haveEqualValues(final MeasurementEntity first, final MeasurementEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

		if (first.getUidMetadata() == null) {
			if (second.getUidMetadata() != null) {
				return false;
			}
		} else if (!first.getUidMetadata().equals(second.getUidMetadata())) {
			return false;
		}

		if (first.getUidSpectrum() == null) {
			if (second.getUidSpectrum() != null) {
				return false;
			}
		} else if (!first.getUidSpectrum().equals(second.getUidSpectrum())) {
			return false;
		}

		if (first.getCertificateEntity() == null) {
			if (second.getCertificateEntity() != null) {
				return false;
			}
		} else if (!CertificateEntity.haveEqualValues(first.getCertificateEntity(),
		                               second.getCertificateEntity())) {
			return false;
		}

		if (first.getConfigurationEntity() == null) {
			if (second.getConfigurationEntity() != null) {
				return false;
			}
		} else if (!ConfigurationEntity.haveEqualValues(first.getConfigurationEntity(),
				second.getConfigurationEntity())) {
			return false;
		}

		if (first.getSampleEntity() == null) {
			if (second.getSampleEntity() != null) {
				return false;
			}
		} else if (!SampleEntity.haveEqualValues(first.getSampleEntity(), second.getSampleEntity())) {
			return false;
		}

		if (first.getSupplementaryMeasurementEntity() == null) {
			if (second.getSupplementaryMeasurementEntity() != null) {
				return false;
			}
		} else if (!SupplementaryMeasurementEntity.haveEqualValues(first.getSupplementaryMeasurementEntity(),
				second.getSupplementaryMeasurementEntity())) {
			return false;
		}

        // for attenuators have to check everything in both sets...
        if (first.getAttenuatorEntites() == null) {
            if (second.getAttenuatorEntites() != null) {
                return false;
            }
        } else {
            if (second.getAttenuatorEntites() == null) {
                return false;
            }
            if (first.getAttenuatorEntites().size() != second.getAttenuatorEntites().size()) {
                return false;
            }
            for (final AttenuatorEntity firstAttenuator : first.getAttenuatorEntites()) {
                for (final AttenuatorEntity secondAttenautor : second.getAttenuatorEntites()) {
                    if (!AttenuatorEntity.haveEqualValues(firstAttenuator, secondAttenautor)) {
                        return false;
                    }
                }
            }
        }
        
		return true;
	}
    
}
