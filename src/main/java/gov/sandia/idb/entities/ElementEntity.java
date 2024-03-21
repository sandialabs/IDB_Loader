package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class ElementEntity {

    private Integer elementId;
    final private String elementName;
    final private String elementSymbol;

    public ElementEntity(final String elementName,
            final String elementSymbol) {
        super();
        this.elementName = elementName;
        this.elementSymbol = elementSymbol;
    }

    public ElementEntity(final Integer elementId,
            final String elementName,
            final String elementSymbol) {
        super();
        this.elementId = elementId;
        this.elementName = elementName;
        this.elementSymbol = elementSymbol;
    }

    public Integer getElementId() {
        return this.elementId;
    }

    public void setElementId(Integer elementId) {
        this.elementId = elementId;
    }

    public String getElementName() {
        return this.elementName;
    }

    public String getElementSymbol() {
        return this.elementSymbol;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((elementId == null) ? 0 : elementId.hashCode());
        result = prime * result + ((elementSymbol == null) ? 0 : elementSymbol.hashCode());
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
        ElementEntity other = (ElementEntity) obj;

        if (elementId != null) {
            return elementId.equals(other.getElementId());
        }
        if (elementId == null) {
            if (other.elementId != null)
                return false;
        } else if (!elementId.equals(other.elementId))
            return false;
        if (elementSymbol == null) {
            if (other.elementSymbol != null)
                return false;
        } else if (!elementSymbol.equals(other.elementSymbol))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ElementEntity [elementId=" + elementId + ", elementName=" + elementName + ", elementSymbol="
                + elementSymbol + "]";
    }

    final private static Map<String, ElementEntity> cache = new HashMap<>();
    final private static String INSERT_NEW_ELEMENT = " INSERT INTO element_table(element_name, element_symbol) "
            + "  VALUES( ?,? );";

    public static final ElementEntity load(final Connection conn, final String elementName,
            final String inputElementSymbol) {
        if (inputElementSymbol == null) {
            return null;
        }

        // See if there is a value in the cache, if so, just return it...
        final String elementSymbol = inputElementSymbol.toUpperCase();
        ElementEntity entity = ElementEntity.cache.get(elementSymbol);
        if (entity != null) {
            return entity;
        }

        // See if there is a value in the table, add it to the cache return it...
        entity = ElementEntity.find(conn, elementSymbol);
        if (entity != null) {
            ElementEntity.cache.put(entity.getElementSymbol(), entity);
            return entity;
        }

        // otherwise make a new booger....
        try (final PreparedStatement statement = conn.prepareStatement(ElementEntity.INSERT_NEW_ELEMENT,
                Statement.RETURN_GENERATED_KEYS)) {

            if (elementName == null) {
                statement.setNull(1, Types.VARCHAR);
            } else {
                statement.setString(1, elementName);
            }
            statement.setString(2, elementSymbol);
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity = new ElementEntity(key,
                            elementName,
                            elementSymbol);
                    ElementEntity.cache.put(entity.getElementSymbol(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting element entity for symbo: " + elementSymbol, e);
        }
        return null;
    }

    final private static String SELECT_ELEMENT_DATA = "SELECT element_id, element_name, element_symbol "
            + " FROM element_table "
            + " WHERE element_symbol = (?); ";

    public static final ElementEntity find(final Connection conn, final String elementSymbol) {

        if (ElementEntity.cache.containsKey(elementSymbol)) {
            return ElementEntity.cache.get(elementSymbol);
        }

        try (final PreparedStatement statement = conn
                .prepareStatement(ElementEntity.SELECT_ELEMENT_DATA)) {
            statement.setString(1, elementSymbol);
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new ElementEntity(rs.getInt("element_id"),
                            rs.getString("element_name"),
                            rs.getString("element_symbol"));
                } else {
                    return null;

                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Element Entity for symbol: " + elementSymbol, e);
        }
    }

    public static boolean haveEqualValues(final ElementEntity first,
            final ElementEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getElementName() == null) {
            if (second.getElementName() != null) {
                return false;
            }
        } else if (!first.getElementName().equals(second.getElementName())) {
            return false;
        }

        if (first.getElementSymbol() == null) {
            if (second.getElementSymbol() != null) {
                return false;
            }
        } else if (!first.getElementSymbol().equals(second.getElementSymbol())) {
            return false;
        }

        return true;
    }
}
