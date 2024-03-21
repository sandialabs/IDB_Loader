package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class IsotopeEntity {

    private Integer isotopeId;
    final private Integer isotopeNumber;
    final private ElementEntity elementEntity;
    final private Float massActivity;

    public IsotopeEntity(Integer isotopeNumber, ElementEntity elementEntity, Float massActivity) {
        super();
        this.isotopeNumber = isotopeNumber;
        this.elementEntity = elementEntity;
        this.massActivity = massActivity;
    }

    public IsotopeEntity(Integer isotopeId, Integer isotopeNumber, ElementEntity elementEntity, Float massActivity) {
        super();
        this.isotopeId = isotopeId;
        this.isotopeNumber = isotopeNumber;
        this.elementEntity = elementEntity;
        this.massActivity = massActivity;
    }

    public Integer getIsotopeId() {
        return isotopeId;
    }

    public void setIsotopeId(Integer isotopeId) {
        this.isotopeId = isotopeId;
    }

    public Integer getIsotopeNumber() {
        return isotopeNumber;
    }

    public ElementEntity getElementEntity() {
        return elementEntity;
    }

    public Float getMassActivity() {
        return massActivity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((elementEntity == null) ? 0 : elementEntity.hashCode());
        result = prime * result + ((isotopeId == null) ? 0 : isotopeId.hashCode());
        result = prime * result + ((isotopeNumber == null) ? 0 : isotopeNumber.hashCode());
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
        IsotopeEntity other = (IsotopeEntity) obj;

        if (isotopeId != null) {
            return isotopeId.equals(other.getIsotopeId());
        }
        if (elementEntity == null) {
            if (other.elementEntity != null)
                return false;
        } else if (!elementEntity.equals(other.elementEntity))
            return false;
        if (isotopeId == null) {
            if (other.isotopeId != null)
                return false;
        } else if (!isotopeId.equals(other.isotopeId))
            return false;
        if (isotopeNumber == null) {
            if (other.isotopeNumber != null)
                return false;
        } else if (!isotopeNumber.equals(other.isotopeNumber))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IsotopeEntity [isotopeId=" + isotopeId + ", isotopeNumber=" + isotopeNumber + ", elementEntity="
                + elementEntity + ", massActivity=" + massActivity + "]";
    }

    final private static Map<Integer, IsotopeEntity> cache = new HashMap<>();
    final private static String INSERT_NEW_ISOTOPE = " INSERT INTO isotope_table("
            + " element_id, isotope_number, isotope_mass_activity) "
            + "  VALUES( ?,?,? );";
    final private static String SELECT_ISOTOPE = " SELECT isotope_id, element_id, isotope_number,"
            + "    isotope_mass_activity "
            + "  FROM isotope_table AS i"
            + "  WHERE i.element_id = (?) "
            + "    AND i.isotope_number = (?);";

    public static final IsotopeEntity load(final Connection conn,
            final ElementEntity elementEntity,
            final Integer isotopeNumber,
            final Float isotopeMassActivity) {
        if (elementEntity == null || isotopeNumber == null) {
            return null;
        }

        // See if there is a value in the table, add it to the cache return it...
        IsotopeEntity entity = IsotopeEntity.find(conn,
                elementEntity,
                isotopeNumber);
        if (entity != null) {
            IsotopeEntity.cache.put(entity.getIsotopeId(), entity);
            return entity;
        }

        // otherwise make a new isotope entry....
        try (final PreparedStatement statement = conn.prepareStatement(IsotopeEntity.INSERT_NEW_ISOTOPE,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, elementEntity.getElementId());
            statement.setInt(2, isotopeNumber);
            if (isotopeMassActivity == null) {
                statement.setNull(3, Types.FLOAT);
            } else {
                statement.setFloat(3, isotopeMassActivity);
            }

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity = new IsotopeEntity(key,
                            isotopeNumber,
                            elementEntity,
                            isotopeMassActivity);
                    IsotopeEntity.cache.put(entity.getIsotopeId(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting element entity for element: " + elementEntity, e);
        }
        return null;
    }

    public static final IsotopeEntity find(final Connection conn,
            final ElementEntity elementEntity,
            final Integer isotopeNumber) {

        // See if there is a value in the cache, if so, just return it...
        for (final IsotopeEntity cachedEntity : IsotopeEntity.cache.values()) {
            if (cachedEntity.getElementEntity().getElementId().equals(elementEntity.getElementId()) &&
                    cachedEntity.getIsotopeNumber().equals(isotopeNumber)) {
                return cachedEntity;
            }
        }

        try (final PreparedStatement statement = conn
                .prepareStatement(IsotopeEntity.SELECT_ISOTOPE)) {
            statement.setInt(1, elementEntity.getElementId());
            statement.setInt(2, isotopeNumber);
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    Float isotopeMassActivity = Float.valueOf(rs.getFloat("isotope_mass_activity"));
                    if (rs.wasNull()) {
                        isotopeMassActivity = null;
                    }

                    final IsotopeEntity entity = new IsotopeEntity(rs.getInt("isotope_id"),
                            isotopeNumber,
                            elementEntity,
                            isotopeMassActivity);

                    // it wasn't in the cache, but was in the database, so add it to the cache...
                    IsotopeEntity.cache.put(entity.getIsotopeId(), entity);
                    return entity;
                } else {
                    return null;

                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Isotope Entity for element: " + elementEntity, e);
        }
    }

    public static IsotopeEntity find(final Integer isotopeId) {
        final IsotopeEntity entity = IsotopeEntity.cache.get(isotopeId);
        // there should always be a in the cache because elements and 
        // isotopes are loaded before any other entity. Throw an exception
        // if there isn't because it means the isotope_id is incorrect or
        // something bad happened during the load of isotope data.
        if (entity == null) {
            throw new RuntimeException("Could not find isotope with id: " + isotopeId);
        }

        return entity;
    }

    public static boolean haveEqualValues(final IsotopeEntity first,
            final IsotopeEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getIsotopeNumber() == null) {
            if (second.getIsotopeNumber() != null) {
                return false;
            }
        } else if (!first.getIsotopeNumber().equals(second.getIsotopeNumber())) {
            return false;
        }

        if (first.getMassActivity() == null) {
            if (second.getMassActivity() != null) {
                return false;
            }
        } else if (!first.getMassActivity().equals(second.getMassActivity())) {
            return false;
        }

        if (first.getElementEntity() == null) {
            if (second.getElementEntity() != null) {
                return false;
            }
        } else if (!ElementEntity.haveEqualValues(first.getElementEntity(), second.getElementEntity())) {
            return false;
        }

        return true;
    }
}
