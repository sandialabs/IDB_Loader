package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class CertificatePlutoniumEntity {

    private Integer certificate_pu_id;
    private CertificateEntity certificateEntity;
    private String reference_date;
    private Float pu_238_value;
    private Float pu_238_sigma;
    private Float pu_239_value;
    private Float pu_239_sigma;
    private Float pu_240_value;
    private Float pu_240_sigma;
    private Float pu_241_value;
    private Float pu_241_sigma;
    private Float pu_242_value;
    private Float pu_242_sigma;
    private Float am_241_value;
    private Float am_241_sigma;
    private Float pu_mass_value;
    private Float pu_mass_sigma;
    private Float pu_content_value;
    private Float pu_content_sigma;
    private Float pu_240eff_value;
    private Float pu_240eff_sigma;
    private String separation_date;

    public String getSeparation_date() {
        return separation_date;
    }

    public void setSeparation_date(String separation_date) {
        this.separation_date = separation_date;
    }

    public Float getPu_240eff_value() {
        return pu_240eff_value;
    }

    public void setReference_date(String reference_date) {
        this.reference_date = reference_date;
    }

    public void setPu_238_value(Float pu_238_value) {
        this.pu_238_value = pu_238_value;
    }

    public void setPu_238_sigma(Float pu_238_sigma) {
        this.pu_238_sigma = pu_238_sigma;
    }

    public void setPu_239_value(Float pu_239_value) {
        this.pu_239_value = pu_239_value;
    }

    public void setPu_239_sigma(Float pu_239_sigma) {
        this.pu_239_sigma = pu_239_sigma;
    }

    public void setPu_240_value(Float pu_240_value) {
        this.pu_240_value = pu_240_value;
    }

    public void setPu_240_sigma(Float pu_240_sigma) {
        this.pu_240_sigma = pu_240_sigma;
    }

    public void setPu_241_value(Float pu_241_value) {
        this.pu_241_value = pu_241_value;
    }

    public void setPu_241_sigma(Float pu_241_sigma) {
        this.pu_241_sigma = pu_241_sigma;
    }

    public void setPu_242_value(Float pu_242_value) {
        this.pu_242_value = pu_242_value;
    }

    public void setPu_242_sigma(Float pu_242_sigma) {
        this.pu_242_sigma = pu_242_sigma;
    }

    public void setAm_241_value(Float am_241_value) {
        this.am_241_value = am_241_value;
    }

    public void setAm_241_sigma(Float am_241_sigma) {
        this.am_241_sigma = am_241_sigma;
    }

    public void setPu_mass_value(Float pu_mass_value) {
        this.pu_mass_value = pu_mass_value;
    }

    public void setPu_mass_sigma(Float pu_mass_sigma) {
        this.pu_mass_sigma = pu_mass_sigma;
    }

    public void setPu_content_value(Float pu_content_value) {
        this.pu_content_value = pu_content_value;
    }

    public void setPu_content_sigma(Float pu_content_sigma) {
        this.pu_content_sigma = pu_content_sigma;
    }

    public void setPu_240eff_value(Float pu_240eff_value) {
        this.pu_240eff_value = pu_240eff_value;
    }

    public Float getPu_240eff_sigma() {
        return pu_240eff_sigma;
    }

    public void setPu_240eff_sigma(Float pu_240eff_sigma) {
        this.pu_240eff_sigma = pu_240eff_sigma;
    }

    public Integer getCertificate_pu_id() {
        return certificate_pu_id;
    }

    public void setCertificate_pu_id(Integer certificate_pu_id) {
        this.certificate_pu_id = certificate_pu_id;
    }

    public CertificateEntity getCertificateEntity() {
        return this.certificateEntity;
    }

    public void setCertificateEntity(final CertificateEntity certificateEntity) {
        this.certificateEntity = certificateEntity;
    }

    public String getReference_date() {
        return reference_date;
    }

    public Float getPu_238_value() {
        return pu_238_value;
    }

    public Float getPu_238_sigma() {
        return pu_238_sigma;
    }

    public Float getPu_239_value() {
        return pu_239_value;
    }

    public Float getPu_239_sigma() {
        return pu_239_sigma;
    }

    public Float getPu_240_value() {
        return pu_240_value;
    }

    public Float getPu_240_sigma() {
        return pu_240_sigma;
    }

    public Float getPu_241_value() {
        return pu_241_value;
    }

    public Float getPu_241_sigma() {
        return pu_241_sigma;
    }

    public Float getPu_242_value() {
        return pu_242_value;
    }

    public Float getPu_242_sigma() {
        return pu_242_sigma;
    }

    public Float getAm_241_value() {
        return am_241_value;
    }

    public Float getAm_241_sigma() {
        return am_241_sigma;
    }

    public Float getPu_mass_value() {
        return pu_mass_value;
    }

    public Float getPu_mass_sigma() {
        return pu_mass_sigma;
    }

    public Float getPu_content_value() {
        return pu_content_value;
    }

    public Float getPu_content_sigma() {
        return pu_content_sigma;
    }

    final private static String NAME_VALUE_PAIR_REFERENCE_DATE = "certificate.pu.reference_date";

    final private static String NAME_VALUE_PAIR_PU_238_VALUE = "certificate.pu.pu.238.value";
    final private static String NAME_VALUE_PAIR_PU_238_SIGMA = "certificate.pu.pu.238.sigma";
    final private static String NAME_VALUE_PAIR_PU_239_VALUE = "certificate.pu.pu.239.value";
    final private static String NAME_VALUE_PAIR_PU_239_SIGMA = "certificate.pu.pu.239.sigma";
    final private static String NAME_VALUE_PAIR_PU_240_VALUE = "certificate.pu.pu.240.value";
    final private static String NAME_VALUE_PAIR_PU_240_SIGMA = "certificate.pu.pu.240.sigma";
    final private static String NAME_VALUE_PAIR_PU_241_VALUE = "certificate.pu.pu.241.value";
    final private static String NAME_VALUE_PAIR_PU_241_SIGMA = "certificate.pu.pu.241.sigma";
    final private static String NAME_VALUE_PAIR_PU_242_VALUE = "certificate.pu.pu.242.value";
    final private static String NAME_VALUE_PAIR_PU_242_SIGMA = "certificate.pu.pu.242.sigma";
    final private static String NAME_VALUE_PAIR_AM_241_VALUE = "certificate.pu.am.241.value";
    final private static String NAME_VALUE_PAIR_AM_241_SIGMA = "certificate.pu.am.241.sigma";

    final private static String NAME_VALUE_PAIR_PU_MASS_VALUE = "supplementary.pu.mass.value";
    final private static String NAME_VALUE_PAIR_PU_MASS_SIGMA = "supplementary.pu.mass.sigma";
    final private static String NAME_VALUE_PAIR_PU_CONTENT_VALUE = "supplementary.pu.content.value";
    final private static String NAME_VALUE_PAIR_PU_CONTENT_SIGMA = "supplementary.pu.content.sigma";

    final private static String NAME_VALUE_PAIR_PU_240EFF_VALUE = "certificate.pu.240eff.value";
    final private static String NAME_VALUE_PAIR_PU_240EFF_SIGMA = "certificate.pu.240eff.sigma";

    final private static String NAME_VALUE_PAIR_SEPARATION_DATE = "certificate.separation_date";
    final private static Map<Integer, CertificatePlutoniumEntity> cache = new HashMap<>();
    final private static String INSERT_CERTIFICATE_PLUTONIUM = " INSERT INTO certificate_pu_table( "
            + " certificate_id, reference_date,  "
            + " pu_238_value, pu_238_sigma, pu_239_value, pu_239_sigma, "
            + " pu_240_value, pu_240_sigma, pu_241_value, pu_241_sigma, "
            + " pu_242_value, pu_242_sigma, am_241_value, am_241_sigma, "
            + " supplementary_pu_mass_value, "
            + " supplementary_pu_mass_sigma, supplementary_pu_content_value, "
            + " supplementary_pu_content_sigma, pu_240eff_value, pu_240eff_sigma, "
            + " separation_date  ) "
            + "  VALUES( ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ? ) ";

    public static final CertificatePlutoniumEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final CertificatePlutoniumEntity entity = CertificatePlutoniumEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Not enough information to make certificate_pu: " + nameValuePairs);
        }
        if (entity.getCertificate_pu_id() != null) {
            return entity;
        }

        try (final PreparedStatement statement = conn.prepareStatement(
                CertificatePlutoniumEntity.INSERT_CERTIFICATE_PLUTONIUM,
                Statement.RETURN_GENERATED_KEYS)) {
            setStatementValues(statement, entity);
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setCertificate_pu_id(key);
                    CertificatePlutoniumEntity.cache.put(entity.getCertificate_pu_id(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting certificate plutonium entity from pairs: " + nameValuePairs, e);
        }

        // if we don't have an entity,
        return null;
    }

    private static void setStatementValues(final PreparedStatement statement,
            final CertificatePlutoniumEntity entity) {
        try {
            statement.setInt(1, entity.getCertificateEntity().getCertificateId());
            setDatetime(2, statement, entity.getReference_date());
            setFloat(3, statement, entity.getPu_238_value());
            setFloat(4, statement, entity.getPu_238_sigma());
            setFloat(5, statement, entity.getPu_239_value());
            setFloat(6, statement, entity.getPu_239_sigma());
            setFloat(7, statement, entity.getPu_240_value());
            setFloat(8, statement, entity.getPu_240_sigma());
            setFloat(9, statement, entity.getPu_241_value());
            setFloat(10, statement, entity.getPu_241_sigma());
            setFloat(11, statement, entity.getPu_242_value());
            setFloat(12, statement, entity.getPu_242_sigma());
            setFloat(13, statement, entity.getAm_241_value());
            setFloat(14, statement, entity.getAm_241_sigma());
            setFloat(15, statement, entity.getPu_mass_value());
            setFloat(16, statement, entity.getPu_mass_sigma());
            setFloat(17, statement, entity.getPu_content_value());
            setFloat(18, statement, entity.getPu_content_sigma());
            setFloat(19, statement, entity.getPu_240eff_value());
            setFloat(20, statement, entity.getPu_240eff_sigma());
            setDatetime(21, statement, entity.getSeparation_date());
        } catch (final Exception e) {
            throw new RuntimeException("Setting statement values...", e);
        }
    }

    private static void setDatetime(final int position,
            final PreparedStatement statement,
            final String dateTimeString) throws SQLException {
        if (dateTimeString == null) {
            statement.setNull(position, Types.TIMESTAMP);
        } else {
            statement.setString(position, dateTimeString);
        }
    }

    private static void setFloat(final int position,
            final PreparedStatement statement,
            final Float floatValue) throws SQLException {
        if (floatValue == null) {
            statement.setNull(position, Types.FLOAT);
        } else {
            statement.setFloat(position, floatValue);
        }
    }

    private static CertificatePlutoniumEntity makeEntity(final Connection conn,
                             final Map<String, String> nameValuePairs) {

        final CertificatePlutoniumEntity entity = new CertificatePlutoniumEntity();
        
                entity.setCertificateEntity(CertificateEntity.load(conn, nameValuePairs));
                entity.setReference_date(getDatetimeValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_REFERENCE_DATE)));
                entity.setPu_238_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_238_VALUE)));
                entity.setPu_238_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_238_SIGMA)));
                entity.setPu_239_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_239_VALUE)));
                entity.setPu_239_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_239_SIGMA)));
                entity.setPu_240_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_240_VALUE)));
                entity.setPu_240_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_240_SIGMA)));
                entity.setPu_241_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_241_VALUE)));
                entity.setPu_241_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_241_SIGMA)));
                entity.setPu_242_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_242_VALUE)));
                entity.setPu_242_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_242_SIGMA)));
                entity.setAm_241_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_AM_241_VALUE)));
                entity.setAm_241_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_AM_241_SIGMA)));
                entity.setPu_mass_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_MASS_VALUE)));
                entity.setPu_mass_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_MASS_SIGMA)));
                entity.setPu_content_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_CONTENT_VALUE)));
                entity.setPu_content_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_CONTENT_SIGMA)));

                entity.setPu_240eff_value(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_240EFF_VALUE)));
                entity.setPu_240eff_sigma(getFloatValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_PU_240EFF_SIGMA)));
                entity.setSeparation_date(getDatetimeValue(nameValuePairs.get(CertificatePlutoniumEntity.NAME_VALUE_PAIR_SEPARATION_DATE)));
                return entity;
    }

    
    public static final CertificatePlutoniumEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final CertificatePlutoniumEntity entity = CertificatePlutoniumEntity.makeEntity(conn, nameValuePairs);

        for (final CertificatePlutoniumEntity cachedEntity : CertificatePlutoniumEntity.cache.values()) {
            if (CertificatePlutoniumEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        return entity;
    }

    public static final boolean haveEqualValues(final CertificatePlutoniumEntity first,
            final CertificatePlutoniumEntity second) {
        // take care of null values first...
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else {
            if (second == null) {
                return false;
            }
        }

        // neither one can be null by this point
        // so check values
        if (first.getCertificateEntity() == null) {
            if (second.getCertificateEntity() != null) {
                return false;
            }
        } else if (!CertificateEntity.haveEqualValues(first.getCertificateEntity(),
                second.getCertificateEntity())) {
            return false;
        }

        if (first.getReference_date() == null) {
            if (second.getReference_date() != null) {
                return false;
            }
        } else if (!first.getReference_date().equals(second.getReference_date())) {
            return false;
        }
        
        if (first.getPu_238_value() == null) {
            if (second.getPu_238_value() != null) {
                return false;
            }
        } else if (!first.getPu_238_value().equals(second.getPu_238_value())) {
            return false;
        }
        
        if (first.getPu_238_sigma() == null) {
            if (second.getPu_238_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_238_sigma().equals(second.getPu_238_sigma())) {
            return false;
        }
        
        if (first.getPu_239_value() == null) {
            if (second.getPu_239_value() != null) {
                return false;
            }
        } else if (!first.getPu_239_value().equals(second.getPu_239_value())) {
            return false;
        }
        
        if (first.getPu_239_sigma() == null) {
            if (second.getPu_239_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_239_sigma().equals(second.getPu_239_sigma())) {
            return false;
        }
        
        if (first.getPu_240_value() == null) {
            if (second.getPu_240_value() != null) {
                return false;
            }
        } else if (!first.getPu_240_value().equals(second.getPu_240_value())) {
            return false;
        }
        
        if (first.getPu_240_sigma() == null) {
            if (second.getPu_240_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_240_sigma().equals(second.getPu_240_sigma())) {
            return false;
        }
        
        if (first.getPu_241_value() == null) {
            if (second.getPu_241_value() != null) {
                return false;
            }
        } else if (!first.getPu_241_value().equals(second.getPu_241_value())) {
            return false;
        }
        
        if (first.getPu_241_sigma() == null) {
            if (second.getPu_241_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_241_sigma().equals(second.getPu_241_sigma())) {
            return false;
        }
        
        if (first.getPu_242_value() == null) {
            if (second.getPu_242_value() != null) {
                return false;
            }
        } else if (!first.getPu_242_value().equals(second.getPu_242_value())) {
            return false;
        }
        
        if (first.getPu_242_sigma() == null) {
            if (second.getPu_242_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_242_sigma().equals(second.getPu_242_sigma())) {
            return false;
        }
      
        if (first.getAm_241_value() == null) {
            if (second.getAm_241_value() != null) {
                return false;
            }
        } else if (!first.getAm_241_value().equals(second.getAm_241_value())) {
            return false;
        }
        
        if (first.getAm_241_sigma() == null) {
            if (second.getAm_241_sigma() != null) {
                return false;
            }
        } else if (!first.getAm_241_sigma().equals(second.getAm_241_sigma())) {
            return false;
        }

        if (first.getPu_mass_value() == null) {
            if (second.getPu_mass_value() != null) {
                return false;
            }
        } else if (!first.getPu_mass_value().equals(second.getPu_mass_value())) {
            return false;
        }
        
        if (first.getPu_mass_sigma() == null) {
            if (second.getPu_mass_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_mass_sigma().equals(second.getPu_mass_sigma())) {
            return false;
        }

        if (first.getPu_content_value() == null) {
            if (second.getPu_content_value() != null) {
                return false;
            }
        } else if (!first.getPu_content_value().equals(second.getPu_content_value())) {
            return false;
        }

        if (first.getPu_content_sigma() == null) {
            if (second.getPu_content_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_content_sigma().equals(second.getPu_content_sigma())) {
            return false;
        }

        if (first.getPu_240eff_value() == null) {
            if (second.getPu_240eff_value() != null) {
                return false;
            }
        } else if (!first.getPu_240eff_value().equals(second.getPu_240eff_value())) {
            return false;
        }

        if (first.getPu_240eff_sigma() == null) {
            if (second.getPu_240eff_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_240eff_sigma().equals(second.getPu_240eff_sigma())) {
            return false;
        }

        if (first.getSeparation_date() == null) {
            if (second.getSeparation_date() != null) {
                return false;
            }
        } else if (!first.getSeparation_date().equals(second.getSeparation_date())) {
            return false;
        }

        return false;
    }

    private static Float getFloatValue(final String value) {
        if (value == null) {
            return null;
        } else {
            return Float.valueOf(value) < 0 ? null : Float.valueOf(value);
            // return Float.valueOf(value);
        }
    }

    // private static Integer getIntegerValue(final String value) {
    // if (value == null) {
    // return null;
    // } else {
    // return Integer.valueOf(value) < 0 ? null : Integer.valueOf(value);
    // }
    // }

    private static String getDatetimeValue(final String value) {
        if (value == null) {
            return null;
        } else {
            if (value.equals("-3")) {
                // return "3333-03-03 03:33:33";
                // IDB-3: NULL out flagged date time
                return null;
            }
            return value;
        }
    }
}
