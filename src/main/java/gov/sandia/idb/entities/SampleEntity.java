package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SampleEntity {
	private Integer sampleId;

	private SampleChemicalCompositionEntity sampleChemicalCompositionEntity;
	private SampleMaterialFormEntity sampleMaterialFormEntity;
	private SourceTypeEntity sourceTypeEntity;
	private Set<SampleMassFractionEntity> sampleMassFractions;
	private Float sampleMoxUPuRatioValue;
	// private Set<MeasurementNoteEntity> notes;

	public SampleEntity(Integer sampleId) {
		super();
		this.sampleId = sampleId;
	}

	public SampleEntity() {
		super();
	}

	public Integer getSampleId() {
		return this.sampleId;
	}

	public void setSampleId(Integer sampleId) {
		this.sampleId = sampleId;
	}

	public SampleChemicalCompositionEntity getSampleChemicalCompositionEntity() {
		return sampleChemicalCompositionEntity;
	}

	public void setSampleChemicalCompositionEntity(SampleChemicalCompositionEntity sampleChemicalCompositionEntity) {
		this.sampleChemicalCompositionEntity = sampleChemicalCompositionEntity;
	}

	public SampleMaterialFormEntity getSampleMaterialFormEntity() {
		return sampleMaterialFormEntity;
	}

	public void setSampleMaterialFormEntity(SampleMaterialFormEntity sampleMaterialFormEntity) {
		this.sampleMaterialFormEntity = sampleMaterialFormEntity;
	}

	public SourceTypeEntity getSourceTypeEntity() {
		return sourceTypeEntity;
	}

	public void setSourceTypeEntity(SourceTypeEntity sourceTypeEntity) {
		this.sourceTypeEntity = sourceTypeEntity;
	}

	public Set<SampleMassFractionEntity> getSampleMassFractions() {
		return sampleMassFractions;
	}

	public void setSampleMassFractions(Set<SampleMassFractionEntity> sampleMassFractions) {
		this.sampleMassFractions = sampleMassFractions;
	}

	public Float getSampleMoxUPuRatioValue() {
		return sampleMoxUPuRatioValue;
	}

	public void setSampleMoxUPuRationEntityValue(final Float sampleMoxUPuRationValue) {
		this.sampleMoxUPuRatioValue = sampleMoxUPuRationValue;
	}
	//
	// public Set<MeasurementNoteEntity> getNotes() {
	// return notes;
	// }
	//
	// public void setNotes(Set<MeasurementNoteEntity> notes) {
	// this.notes = notes;
	// }

	public void setSampleMoxUPuRatioValue(Float sampleMoxUPuRatioValue) {
		this.sampleMoxUPuRatioValue = sampleMoxUPuRatioValue;
	}

	@Override
	public String toString() {
		return "SampleEntity [sampleId=" + sampleId + ", sampleChemicalCompositionEntity="
				+ sampleChemicalCompositionEntity + ", sampleMaterialFormEntity=" + sampleMaterialFormEntity
				+ ", sourceTypeEntity=" + sourceTypeEntity + ", sampleMassFractions=" + sampleMassFractions
				+ ", sampleMoxUPuRation=" + sampleMoxUPuRatioValue + "]";
	}

	final static private Map<Integer, SampleEntity> cache = new HashMap<>();
	final private static String NAME_VALUE_PAIR_KEY = "sample.material.type";
	final private static String MOX_U_PU_RATIO_PAIR_KEY = "sample.mox.u_pu_ratio.value";

	final private static String INSERT_SAMPLE = " INSERT INTO sample_table( "
			+ "   sample_chemical_composition_id, source_type_id, sample_material_form_id, sample_mox_u_pu_ratio_value ) "
			+ "  VALUES( ?,?,?,? );";

	final private static String SELECT_SAMPLE = " SELECT sample_id "
			+ " FROM sample_table AS s "
			+ " WHERE sample_chemical_composition_id <=> (?) "
			+ "   AND source_type_id = (?) "
			+ "   AND sample_material_form_id <=> (?) "
			+ "   AND sample_mox_u_pu_ratio_value <=> (?); ";

	static final public SampleEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

		final SampleEntity entity = SampleEntity.find(conn, nameValuePairs);
		if (entity == null) {
			throw new RuntimeException("Could not find or create sample entity..." + nameValuePairs);
		}

		// if the returned entity has an id, then it has already been loaded
		// into the database, so just return it.
		if (entity.getSampleId() != null) {
			return entity;
		}

		// otherwise, this is a new sample, so write it to the database,
		// set the return id field, load into the cache, and return
		// the new sample...
		try (final PreparedStatement statement = conn.prepareStatement(SampleEntity.INSERT_SAMPLE,
				Statement.RETURN_GENERATED_KEYS)) {
			setFieldValuesIntoPreparedStatement(statement, entity);
			statement.executeUpdate();
			try (final ResultSet rs = statement.getGeneratedKeys()) {
				if (rs.next()) {
					final Integer key = Integer.valueOf(rs.getInt(1));
					if (rs.wasNull()) {
						throw new RuntimeException("Key is null.");
					}
					entity.setSampleId(key);
					SampleEntity.cache.put(key, entity);
				}
			}

		} catch (final Exception e) {
			throw new RuntimeException("Inserting sample: " + nameValuePairs, e);
		}

		// Mass fractions are treated as part of the sample, so they
		// are not saved or checked for duplicates when created
		// from CSV data. Save them here after the sample_id is
		// available.
		for (final SampleMassFractionEntity sampleMassFraction : entity.getSampleMassFractions()) {
			sampleMassFraction.setSampleId(entity.getSampleId());
			SampleMassFractionEntity.load(conn, sampleMassFraction);
		}

		return entity;
	}

	public static final SampleEntity find(final Connection conn,
			final Map<String, String> nameValuePairs) {

		String sampleMaterialTypeName = nameValuePairs.get(SampleEntity.NAME_VALUE_PAIR_KEY);
		final SampleEntity entity = new SampleEntity();

		entity.setSourceTypeEntity(SourceTypeEntity.get(sampleMaterialTypeName));
		entity.setSampleMaterialFormEntity(SampleMaterialFormEntity.load(conn, nameValuePairs));
		entity.setSampleChemicalCompositionEntity(SampleChemicalCompositionEntity.load(conn, nameValuePairs));
		entity.setSampleMassFractions(SampleMassFractionEntity.load(conn, nameValuePairs));

		String sampleMoxUPuRatioString = nameValuePairs
				.get(SampleEntity.MOX_U_PU_RATIO_PAIR_KEY);
		if (sampleMoxUPuRatioString == null) {
			entity.setSampleMoxUPuRationEntityValue(null);
		} else {
			final Float moxUPuRatioValue = Float.valueOf(sampleMoxUPuRatioString.trim());
			entity.setSampleMoxUPuRationEntityValue(moxUPuRatioValue);
		}

		for (final SampleEntity cachedEntity : SampleEntity.cache.values()) {
			if (SampleEntity.haveEqualValues(entity, cachedEntity)) {
				return cachedEntity;
			}
		}

		// now look in the database. First check to see if
		// there is already a sample row with current values.
		// If not, then this is a new sample so return. If so,
		// then need to check the mass fractions. Call a
		// convenience method that loads the mass fractions
		// for the retrieved sample that has equal values.
		try (final PreparedStatement statement = conn
				.prepareStatement(SampleEntity.SELECT_SAMPLE)) {
			setFieldValuesIntoPreparedStatement(statement, entity);
			try (final ResultSet rs = statement.executeQuery()) {
				if (rs.next()) {
					// found a record, check if mass fractions are equal by:
					// Creating a new sample. Set the sample values to
					// be the same as those in the entity because the
					// database retrieval has checked that already.
					// Use the returned sample_id to find all mass
					// fractions associated with this sample. Then
					// use the haveEqualValues method to see if all
					// the sample mass fractions are the same.
					final SampleEntity databaseEntity = new SampleEntity();
					databaseEntity.setSampleId(rs.getInt("sample_id"));
					databaseEntity.setSampleChemicalCompositionEntity(entity.getSampleChemicalCompositionEntity());
					databaseEntity.setSampleMaterialFormEntity(entity.getSampleMaterialFormEntity());
					databaseEntity.setSourceTypeEntity(entity.getSourceTypeEntity());
					databaseEntity.setSampleMoxUPuRatioValue(entity.getSampleMoxUPuRatioValue());
					databaseEntity.setSampleMassFractions(
							SampleMassFractionEntity.findSampleMassFractions(conn, databaseEntity));

					if (haveEqualValues(entity, databaseEntity)) {
						return databaseEntity;
					}

				}
			}
		} catch (final Exception e) {
			throw new RuntimeException("Looking up supplementary measurement: " + entity, e);
		}
		// this sample entity has new values that need to be added to the
		// sample table, so return the non-cached entity...
		return entity;
	}

	public static boolean haveEqualValues(final SampleEntity first, final SampleEntity second) {
		if (first == null) {
			if (second == null) {
				return true;
			} else {
				return false;
			}
		} else if (second == null) {
			return false;
		}

		if (first.getSampleMoxUPuRatioValue() == null) {
			if (second.getSampleMoxUPuRatioValue() != null) {
				return false;
			}
		} else if (!first.getSampleMoxUPuRatioValue().equals(second.getSampleMoxUPuRatioValue())) {
			return false;
		}

		if (first.getSampleChemicalCompositionEntity() == null) {
			if (second.getSampleChemicalCompositionEntity() != null) {
				return false;
			}
		} else if (!SampleChemicalCompositionEntity.haveEqualValues(first.getSampleChemicalCompositionEntity(),
				second.getSampleChemicalCompositionEntity())) {
			return false;
		}

		if (first.getSampleMaterialFormEntity() == null) {
			if (second.getSampleMaterialFormEntity() != null) {
				return false;
			}
		} else if (!SampleMaterialFormEntity.haveEqualValues(first.getSampleMaterialFormEntity(),
				second.getSampleMaterialFormEntity())) {
			return false;
		}

		if (first.getSourceTypeEntity() == null) {
			if (second.getSourceTypeEntity() != null) {
				return false;
			}
		} else if (!SourceTypeEntity.haveEqualValues(first.getSourceTypeEntity(),
				second.getSourceTypeEntity())) {
			return false;
		}

		// Mass fractions need to compare the values in the set. The
		// initial if checks if the first is null and the second is
		// not null; it will fall through to the else if when both
		// the first and second are null. However, it will also
		// fall through if both the first and second are not null
		// so we need to have the else if so that when both are not
		// null we also check that every SampleMassFraction in both
		// sets has equal non-id values.
		if (first.getSampleMassFractions() == null) {
			if (second.getSampleMassFractions() != null) {
				return false;
			}
		} else if (first.getSampleMassFractions() != null) {
			final Set<SampleMassFractionEntity> firstMassFractions = first.getSampleMassFractions();
			final Set<SampleMassFractionEntity> secondMassFractions = second.getSampleMassFractions();
			if (firstMassFractions.size() != secondMassFractions.size()) {
				return false;
			}
			// If the sets are the same size and we loop through every value in one set
			// to see if it has an sample mass fraction with the same values in the other
			// set. We can't use the equals or the contains because sample mass fractions
			// from the cached sample will have a database id and the sample mass fractions
			// in the first sample will not because we are reading from the input data.
			boolean found;
			for (final SampleMassFractionEntity firstMassFraction : firstMassFractions) {
				found = false;
				for (final SampleMassFractionEntity secondMassFraction : secondMassFractions) {
					if (SampleMassFractionEntity.haveEqualValues(firstMassFraction, secondMassFraction)) {
						// if there are equal values, then break out of loop
						// over second set and go on to the next mass fraction
						// in the first set
						found = true;
						break;
					}
				}
				if (!found) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Convenience method so loading into statement isn't
	 * duplicated in the insert and when finding...
	 * 
	 */
	private static void setFieldValuesIntoPreparedStatement(final PreparedStatement statement,
			final SampleEntity entity) {
		try {
			if (entity.getSampleChemicalCompositionEntity() == null) {
				statement.setNull(1, Types.INTEGER);
			} else {
				statement.setInt(1, entity.getSampleChemicalCompositionEntity().getSampleChemicalCompositionId());
			}

			// If source type is null, an exception should be thrown...
			statement.setInt(2, entity.getSourceTypeEntity().getSourceTypeId());

			if (entity.getSampleMaterialFormEntity() == null) {
				statement.setNull(3, Types.INTEGER);
			} else {
				statement.setInt(3, entity.getSampleMaterialFormEntity().getSampleMaterialFormId());
			}

			if (entity.getSampleMoxUPuRatioValue() == null) {
				statement.setNull(4, Types.FLOAT);
			} else {
				statement.setFloat(4, entity.getSampleMoxUPuRatioValue());
			}
		} catch (final Exception e) {
			throw new RuntimeException("Setting values into prepared statement: "
					+ entity, e);
		}
	}

}
