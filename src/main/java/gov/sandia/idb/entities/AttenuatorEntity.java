package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AttenuatorEntity {

    private Integer attenuatorId;
    private String material;
    private Float thickness_mm;
    private Boolean approximation;
    private Float minimum_mm;
    private Float maximum_mm;

    public Integer getAttenuatorId() {
        return attenuatorId;
    }

    public void setAttenuatorId(Integer attenuatorId) {
        this.attenuatorId = attenuatorId;
    }

    public String getMaterial() {
        return material;
    }

    public void setMaterial(String material) {
        this.material = material;
    }

    public Float getThickness_mm() {
        return thickness_mm;
    }

    public void setThickness_mm(final Float thickness_mm) {
        this.thickness_mm = thickness_mm;
    }

    public Boolean getApproximation() {
        return approximation;
    }

    public void setApproximation(Boolean approximation) {
        this.approximation = approximation;
    }

    public Float getMinimum_mm() {
        return minimum_mm;
    }

    public void setMinimum_mm(final Float minimum_mm) {
        this.minimum_mm = minimum_mm;
    }

    public Float getMaximum_mm() {
        return maximum_mm;
    }

    public void setMaximum_mm(final Float maximum_mm) {
        this.maximum_mm = maximum_mm;
    }

    @Override
    public String toString() {
        return "AttenuatorEntity [attenuatorId=" + attenuatorId + ", material=" + material + ", thickness_mm="
                + thickness_mm + ", approximation=" + approximation + ", minimum_mm=" + minimum_mm + ", maximum_mm="
                + maximum_mm + "]";
    }

    final private static String ATTENUATOR_NAME_VALUE_PAIR_KEY = "configuration.attenuators";
    final private static Map<Integer, AttenuatorEntity> cache = new HashMap<>();
    final private static String INSERT_ATTENUATOR = " INSERT INTO attenuator_table( "
            + "   material, thickness_mm, approximation, minimum_mm, maximum_mm ) "
            + "  VALUES( ?,?,?,?,? );";

    final private static String SELECT_ATTENUATOR_BY_VALUES = " SELECT "
            + " attenuator_id, material, thickness_mm, approximation, minimum_mm, maximum_mm "
            + " FROM attenuator_table AS a "
            + " WHERE a.material <=> (?); ";
//            + "   AND a.thickness_mm = (?) "
//            + "   AND a.approximation = (?) "
//            + "   AND a.minimum_mm = (?) "
//            + "   AND a.maximum_mm = (?);";

    public static final Set<AttenuatorEntity> load(final Connection conn, final Map<String, String> nameValuePairs) {

        Set<AttenuatorEntity> entities = find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entities == null || entities.size() == 0) {
            return null;
        }

        // now loop over all returned entities and
        // if there is a new value for attenuator data
        // add it to the database. The returned set
        // should contain attenuator entities that all
        // have an attenuator table id...
        for (final AttenuatorEntity entity : entities) {
            // found entity in cache, so skip creation...
            if (entity.getAttenuatorId() != null) {
                continue;
            }

            // need to create a new entity...
            try (final PreparedStatement statement = conn.prepareStatement(AttenuatorEntity.INSERT_ATTENUATOR,
                    Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, entity.getMaterial());

                if (entity.getThickness_mm() == null) {
                    statement.setNull(2, Types.FLOAT);
                } else {
                    statement.setFloat(2, entity.getThickness_mm());
                }
                if (entity.getApproximation() == null) {
                    statement.setNull(3, Types.VARCHAR);
                } else {
                    statement.setBoolean(3, entity.getApproximation());
                }

                if (entity.getMinimum_mm() == null) {
                    statement.setNull(4, Types.FLOAT);
                } else {
                    statement.setFloat(4, entity.getMinimum_mm());
                }
                if (entity.getMaximum_mm() == null) {
                    statement.setNull(5, Types.VARCHAR);
                } else {
                    statement.setFloat(5, entity.getMaximum_mm());
                }
                statement.executeUpdate();
                try (final ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        final Integer key = Integer.valueOf(rs.getInt(1));
                        if (rs.wasNull()) {
                            throw new RuntimeException("Key is null.");
                        }
                        entity.setAttenuatorId(key);
                        AttenuatorEntity.cache.put(entity.getAttenuatorId(), entity);
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException("Inserting attenautor from pairs: " + nameValuePairs, e);
            }
        }
        return entities;
    }

    private static final Set<AttenuatorEntity> find(final Connection conn, final Map<String, String> nameValuePairs) {
        final Set<AttenuatorEntity> parsedEntities = AttenuatorEntity
                .parseAttenuatorString(nameValuePairs.get(AttenuatorEntity.ATTENUATOR_NAME_VALUE_PAIR_KEY));
        if (parsedEntities == null) {
            return null;
        }

        // if the values of the attenuator entity are already in the cache, then
        // it is a duplicate and we want to use the cached values. It is possible
        // that a line will have some attenuators that are already in the database
        // and some that are not, so there is this complicated comparison of
        // every entry in the newly parsed attenuators to see if there is
        // already a value in the cache. The returned set will, then, possibly
        // contain some attenuator entities with a database id and some without
        final Set<AttenuatorEntity> returnedEntities = new HashSet<>();
        boolean foundInCache = false;
        for (final AttenuatorEntity parsedEntity : parsedEntities) {
            foundInCache = false;
            for (final AttenuatorEntity cachedEntity : AttenuatorEntity.cache.values()) {
                if (haveEqualValues(parsedEntity, cachedEntity)) {

                    returnedEntities.add(cachedEntity);
                    foundInCache = true;
                    break;
                }
            }
            // This particular parsed attenuator isn't in the cache,
            // check if it is the database. If it is, then add the
            // database version to the cache and as the returned entity.
            // If not, then this is a new entry, so add the parsed
            // entity (i.e., without a db id) to the returned entities
            // so a database entry will be created...
            if (!foundInCache) {
                final AttenuatorEntity foundEntity = AttenuatorEntity.findInDatabase(conn, parsedEntity);
                if (foundEntity != null) {
                    returnedEntities.add(foundEntity);
                } else {
                    returnedEntities.add(parsedEntity);
                }
            }

        }

        return returnedEntities;
    }

    /**
     * Convenience method that searches the database to see if a attenuator with the
     * same values (i.e., non-id fields) already exists in the database
     * 
     * @param parsedEntity
     * @return
     */
    private static AttenuatorEntity findInDatabase(final Connection conn, final AttenuatorEntity parsedEntity) {

        try (final PreparedStatement statement = conn.prepareStatement(AttenuatorEntity.SELECT_ATTENUATOR_BY_VALUES)) {
            if (parsedEntity.getMaterial() == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, parsedEntity.getMaterial());
            }
//            if (parsedEntity.getThickness_mm() == null) {
//                statement.setNull(2, Types.FLOAT);
//            } else {
//                statement.setFloat(2, parsedEntity.getThickness_mm());
//            }
//            if (parsedEntity.getApproximation() == null) {
//                statement.setNull(3, Types.BOOLEAN);
//            } else {
//                statement.setBoolean(3, parsedEntity.getApproximation().booleanValue());
//            }
//            if (parsedEntity.getMinimum_mm() == null) {
//                statement.setNull(4, Types.FLOAT);
//            } else {
//                statement.setFloat(4, parsedEntity.getMinimum_mm());
//            }
//            if (parsedEntity.getMaximum_mm() == null) {
//                statement.setNull(5, Types.FLOAT);
//            } else {
//                statement.setFloat(5, parsedEntity.getMaximum_mm());
//            }

            try (final ResultSet rs = statement.executeQuery()) {
                 while (rs.next()) {
                    final AttenuatorEntity foundEntity = new AttenuatorEntity();
                    foundEntity.setAttenuatorId(rs.getInt("attenuator_id"));
                    
                    foundEntity.setMaterial(rs.getString("material"));
                    final Float thickness = Float.valueOf(rs.getFloat("thickness_mm"));
                    if (!rs.wasNull()) {
                        foundEntity.setThickness_mm(thickness);
                    } else {
                        foundEntity.setThickness_mm(null);
                    }
                    final Boolean approximation = rs.getBoolean("approximation");
                    if (!rs.wasNull()) {
                        foundEntity.setApproximation(approximation);
                    } else {
                        foundEntity.setApproximation(null);
                    }
                    final Float minimum_mm = Float.valueOf(rs.getFloat("minimum_mm"));
                    if (!rs.wasNull()) {
                        foundEntity.setMinimum_mm(minimum_mm);
                    } else {
                        foundEntity.setMinimum_mm(null);
                    }
                    final Float maximum_mm = Float.valueOf(rs.getFloat("maximum_mm"));
                    if (!rs.wasNull()) {
                        foundEntity.setMaximum_mm(maximum_mm);
                    } else {
                        foundEntity.setMaximum_mm(null);
                    }
                    
                    if (AttenuatorEntity.haveEqualValues(parsedEntity, foundEntity)) {
                        AttenuatorEntity.cache.put(foundEntity.getAttenuatorId(), foundEntity);
                        return foundEntity;
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Searching for attenuator: " + parsedEntity);
        }

        // nothing found, return null
        return null;
    }

    public static boolean haveEqualValues(final AttenuatorEntity first,
            final AttenuatorEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getMaterial() == null) {
            if (second.getMaterial() != null) {
                return false;
            }
        } else if (!first.getMaterial().equals(second.getMaterial())) {
            return false;
        }

        if (first.getThickness_mm() == null) {
            if (second.getThickness_mm() != null) {
                return false;
            }
        } else if (!first.getThickness_mm().equals(second.getThickness_mm())) {
            return false;
        }

        if (first.getApproximation() == null) {
            if (second.getApproximation() != null) {
                return false;
            }
        } else if (!first.getApproximation().equals(second.getApproximation())) {
            return false;
        }

        if (first.getMinimum_mm() == null) {
            if (second.getMinimum_mm() != null) {
                return false;
            }
        } else if (!first.getMinimum_mm().equals(second.getMinimum_mm())) {
            return false;
        }

        if (first.getMaximum_mm() == null) {
            if (second.getMaximum_mm() != null) {
                return false;
            }
        } else if (!first.getMaximum_mm().equals(second.getMaximum_mm())) {
            return false;
        }

        return true;
    }

    /**
     * Parse an attenuator string that can have different values. The first part of
     * the string is always the material type followed by a colon. The colon is
     * followed either by a tilda (~) if the thickness value is approximate or just
     * by the value in mm. Some thickness values are given as a range of min to max
     * thickness and in those cases, there is not a tilda, but the first number is
     * the minimum thickness, then there is a hyphen (-), followed by the maximum
     * thickness. All thicknesses are in mm.
     * 
     * The format is given by:
     * 
     * [material]:[~ if approximate value][thickness mm][- if min/max thickness][max
     * thickness if min/max]
     * 
     * The square brackets are not in the string, but are included in the format
     * description to make it easier to see what fields occur when.
     * 
     * @param attenuatorString
     * @return
     */
    final public static Set<AttenuatorEntity> parseAttenuatorString(final String attenuatorString) {
        final Set<AttenuatorEntity> entities = new HashSet<>();
        AttenuatorEntity entity = new AttenuatorEntity();

        if (attenuatorString == null) {
            entity.setMaterial(null);
            entity.setApproximation(null);
            entity.setThickness_mm(null);
            entity.setMaximum_mm(null);
            entity.setMinimum_mm(null);
            entities.add(entity);
            return entities;
        } else if (attenuatorString.equalsIgnoreCase("None")) {
            entity.setMaterial("NONE");
            entity.setApproximation(Boolean.FALSE);
            entity.setThickness_mm(0.0f);
            entity.setMaximum_mm(null);
            entity.setMinimum_mm(null);
            entities.add(entity);
            return entities;
        } else if (attenuatorString.trim().equalsIgnoreCase("UNKNOWN")) {
            entity.setMaterial(null);
            entity.setApproximation(null);
            entity.setThickness_mm(null);
            entity.setMaximum_mm(null);
            entity.setMinimum_mm(null);
            entities.add(entity);
            return entities;
        } else if (attenuatorString.isEmpty()) {
            return null;
        }

        final String[] materialThicknessSplit = attenuatorString.split(":");
        if (materialThicknessSplit.length % 2 != 0) {
            throw new RuntimeException("Attenuator string format, colon incorrectly placed: " + attenuatorString);
        }

        int i = 0;
        while (i < materialThicknessSplit.length) {
            // Everything before the colon, is the attenuator material...
            entity.setMaterial(materialThicknessSplit[i]);

            // if the first character in the thickness part of the string is a tilda, then
            // this is an approximate value...
            entity.setApproximation(materialThicknessSplit[i + 1].startsWith("~"));

            // As a convenience get rid of the tilda at the beginning
            // of the thickness definition if it exists...
            String thicknessString = null;
            if (materialThicknessSplit[i + 1].startsWith("~")) {
                thicknessString = materialThicknessSplit[i + 1].substring(1);
            } else {
                thicknessString = materialThicknessSplit[i + 1];
            }

            // now split on hyphen to see if this is an approximate thickness
            // with min max values...
            final String[] minMaxSplit = thicknessString.split("-");

            if (minMaxSplit.length == 2) {
                entity.setApproximation(Boolean.TRUE);
                entity.setThickness_mm(null);
                entity.setMinimum_mm(Float.valueOf(minMaxSplit[0]));
                entity.setMaximum_mm(Float.valueOf(minMaxSplit[1]));
            } else {
                entity.setThickness_mm(Float.valueOf(thicknessString));
                entity.setMinimum_mm(null);
                entity.setMaximum_mm(null);
            }
            i += 2;
            entities.add(entity);
            entity = new AttenuatorEntity();
        }
        return entities;
    }
}
