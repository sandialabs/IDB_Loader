package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SampleMassFractionEntity {

    private Integer sampleMassFractionId;
    private Integer sampleId;
    private IsotopeEntity isotopeEntity;
    private Float massFraction;
    private String notes;

    public SampleMassFractionEntity() {

    }

    public SampleMassFractionEntity(
            final Integer sampleId,
            IsotopeEntity isotopeEntity,
            Float isotopeMassFraction,
            String notes) {
        super();
        this.sampleId = sampleId;
        this.isotopeEntity = isotopeEntity;
        this.massFraction = isotopeMassFraction;
        this.notes = notes;
    }

    public SampleMassFractionEntity(
            final Integer sampleMassFractionId,
            final Integer sampleId,
            IsotopeEntity isotopeEntity,
            Float isotopeMassFraction,
            String notes) {
        super();
        this.sampleMassFractionId = sampleMassFractionId;
        this.sampleId = sampleId;
        this.isotopeEntity = isotopeEntity;
        this.massFraction = isotopeMassFraction;
        this.notes = notes;
    }

    public Integer getSampleMassFractionId() {
        return sampleMassFractionId;
    }

    public void setSampleMassFractionId(Integer sampleMassFractionId) {
        this.sampleMassFractionId = sampleMassFractionId;
    }

    public Integer getSampleId() {
        return this.sampleId;
    }

    public void setSampleId(final Integer sampleId) {
        this.sampleId = sampleId;
    }

    public IsotopeEntity getIsotopeEntity() {
        return isotopeEntity;
    }

    public Float getMassFraction() {
        return massFraction;
    }

    public String getNotes() {
        return notes;
    }

    public void setIsotopeEntity(IsotopeEntity isotopeEntity) {
        this.isotopeEntity = isotopeEntity;
    }

    public void setMassFraction(Float massFraction) {
        this.massFraction = massFraction;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((isotopeEntity == null) ? 0 : isotopeEntity.hashCode());
        result = prime * result + ((massFraction == null) ? 0 : massFraction.hashCode());
        result = prime * result + ((sampleMassFractionId == null) ? 0 : sampleMassFractionId.hashCode());
        result = prime * result + ((notes == null) ? 0 : notes.hashCode());
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
        SampleMassFractionEntity other = (SampleMassFractionEntity) obj;

        if (this.sampleMassFractionId != null) {
            return this.sampleMassFractionId.equals(other.getSampleMassFractionId());
        }
        if (isotopeEntity == null) {
            if (other.isotopeEntity != null)
                return false;
        } else if (!isotopeEntity.equals(other.isotopeEntity))
            return false;
        if (massFraction == null) {
            if (other.massFraction != null)
                return false;
        } else if (!massFraction.equals(other.massFraction))
            return false;
        if (sampleMassFractionId == null) {
            if (other.sampleMassFractionId != null)
                return false;
        } else if (!sampleMassFractionId.equals(other.sampleMassFractionId))
            return false;
        if (notes == null) {
            if (other.notes != null)
                return false;
        } else if (!notes.equals(other.notes))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SampleMassFractionEntity [sampleMassFractionId=" + sampleMassFractionId + ", isotopeEntity="
                + isotopeEntity + ", massFraction=" + massFraction
                + ", notes=" + notes + "]";
    }

    // ----------------------------------------------------
    // sample compositions in CSV files are always specified like:
    // sample.composition.pu.238
    // To get the element and isotope number, split the CSV string
    // on periods to end up with an array of four strings. Assuming
    // 0 based array index, the element name is the third entry
    // (second index) in the array and the isotope number is the
    // fourth entry (third index) in the array.
    // ----------------------------------------------------------
    final private static String NAME_VALUE_PAIR_STARTS_KEY = "sample.composition";
    final private static Integer NAME_VALUE_PAIR_ELEMENT_SYMBOL = 2;
    final private static Integer NAME_VALUE_PAIR_ISOTOPE_NUMBER = 3;

    public static final String SELECT_MASS_FRACTIONS = " SELECT mass_fraction_id, "
            + "   sample_id, isotope_id, isotope_mass_fraction, isotope_mass_fraction_notes "
            + " FROM sample_mass_fraction_table AS m "
            + " WHERE m.sample_id = (?);  ";

    private static final String INSERT_MASS_FRACTION = " INSERT INTO sample_mass_fraction_table( "
            + " sample_id, isotope_id, isotope_mass_fraction, isotope_mass_fraction_notes ) "
            + " VALUES( ?,?,?,? ) ";

    // private static final String MASS_FRACTION_SAMPLE_ID_UPDATE = " UPDATE
    // sample_mass_fraction_table "
    // + " set sample_id = (?) "
    // + " WHERE sample_id = (?); ";

    /**
     * 
     * @param conn
     * @param entity
     */
    public static void load(final Connection conn,
            final SampleMassFractionEntity entity) {

        // otherwise, this is new mass fraction, so insert it..
        try (final PreparedStatement statement = conn.prepareStatement(
                SampleMassFractionEntity.INSERT_MASS_FRACTION,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, entity.getSampleId());
            statement.setInt(2, entity.getIsotopeEntity().getIsotopeId());
            if (entity.getMassFraction() == null) {
                statement.setNull(3, Types.FLOAT);
            } else {
                statement.setFloat(3, entity.getMassFraction());
            }
            if (entity.getNotes() == null) {
                statement.setNull(4, Types.VARCHAR);
            } else {
                statement.setString(4, entity.getNotes());
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting sample mass fraction entity for sample " + entity.getSampleId(), e);
        }

    }

    /**
     * Sample mass fractions are treated as if they are part of the
     * sample entity when checking for duplicate entries. This means
     * that sample mass fraction values might be duplicated in the
     * table, but the relation between the sample and its mass
     * fractions is so tightly coupled that it seams better to
     * duplicate the mass fractions in the table rather than
     * needing a many-to-many relationship between the sample
     * table and the sample mass fraction table.
     * 
     * It also means that the sample mass fraction entity does
     * not have a cache becasue we cache the values as part
     * of the sample. It also means that the load from the
     * name value pairs, just loads the CSV data, it does
     * not try to find exising records in the cache or
     * the database. That check is done in the sample entity.
     * 
     * Finally, when the sample is saved to the database,
     * the
     * 
     * @param conn
     * @param nameValuePairs
     * @return
     */
    public static Set<SampleMassFractionEntity> load(final Connection conn,
            final Map<String, String> nameValuePairs) {
        final Set<SampleMassFractionEntity> entities = new HashSet<>();

        for (final Entry<String, String> nameValueEntry : nameValuePairs.entrySet()) {
            // skip this name/value pair if it doesn't start with the correct header...
            if (!nameValueEntry.getKey().startsWith(SampleMassFractionEntity.NAME_VALUE_PAIR_STARTS_KEY)) {
                continue;
            }

            final String[] names = nameValueEntry.getKey().split("\\.");
            final String elementSymbol = names[SampleMassFractionEntity.NAME_VALUE_PAIR_ELEMENT_SYMBOL].trim()
                    .toUpperCase();
            final Integer isotopeNumber = Integer
                    .valueOf(names[SampleMassFractionEntity.NAME_VALUE_PAIR_ISOTOPE_NUMBER]);
            String isotopeMassFractionString = nameValueEntry.getValue();
            String isotopeMassFractionNotes = null;
            Float isotopeMassFraction = isotopeMassFractionString == null ? null
                    : Float.valueOf(isotopeMassFractionString);

            final ElementEntity elementEntity = ElementEntity.load(conn, "", elementSymbol);
            final IsotopeEntity isotopeEntity = IsotopeEntity.load(conn,
                    elementEntity,
                    isotopeNumber,
                    null);

            entities.add(new SampleMassFractionEntity(null,
                    null,
                    isotopeEntity,
                    isotopeMassFraction,
                    isotopeMassFractionNotes));

        }

        return entities;
    }

    public static boolean haveEqualValues(final SampleMassFractionEntity first,
            final SampleMassFractionEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getMassFraction() == null) {
            if (second.getMassFraction() != null) {
                return false;
            }
        } else if (!first.getMassFraction().equals(second.getMassFraction())) {
            return false;
        }

        if (first.getIsotopeEntity() == null) {
            if (second.getIsotopeEntity() != null) {
                return false;
            }
        } else if (!IsotopeEntity.haveEqualValues(first.getIsotopeEntity(), second.getIsotopeEntity())) {
            return false;
        }

        return true;
    }

    /**
     * Convenience method that finds all sample mass fractions associated
     * with the sample_id in the input sample entity.
     * 
     * 
     */
    public static Set<SampleMassFractionEntity> findSampleMassFractions(final Connection conn,
            final SampleEntity sample) {

        final Set<SampleMassFractionEntity> fractions = new HashSet<>();
        try (final PreparedStatement statement = conn
                .prepareStatement(SampleMassFractionEntity.SELECT_MASS_FRACTIONS)) {

            statement.setInt(1, sample.getSampleId());
            try (final ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    final SampleMassFractionEntity fraction = new SampleMassFractionEntity();
                    fraction.setSampleMassFractionId(rs.getInt("mass_fraction_id"));
                    fraction.setSampleId(rs.getInt("sample_id"));

                    fraction.setIsotopeEntity(IsotopeEntity.find(rs.getInt("isotope_id")));
                    fraction.setMassFraction(rs.getFloat("isotope_mass_fraction"));
                    if (rs.wasNull()) {
                        fraction.setMassFraction(null);
                    }
                    fractions.add(fraction);
                }
            }

            return fractions;
        } catch (final Exception e) {
            throw new RuntimeException("Finding mass fractions for sample: " + sample, e);
        }

    }
}
