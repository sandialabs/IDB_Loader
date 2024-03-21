package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class CertificateMOXEntity {
    private Integer certificate_mox_id;
    private CertificateEntity certificateEntity;
    private String reference_date;
    private Float pu238_value;
    private Float pu238_sigma;
    private Float pu239_value;
    private Float pu239_sigma;
    private Float pu240_value;
    private Float pu240_sigma;
    private Float pu241_value;
    private Float pu241_sigma;
    private Float pu242_value;
    private Float pu242_sigma;
    private Float am241_value;
    private Float am241_sigma;
    private Float mox_u_percent;
    private Float mox_pu_percent;
    private Float mox_u_pu_ratio_value;
    private Float mox_u_pu_ratio_sigma;
    private Float mox_u_234_value;
    private Float pu_mass_value;
    private Float pu_mass_sigma;
    private Float pu_content_value;
    private Float pu_content_sigma;
    private Float mox_u_234_sigma;
    private Float mox_u_235_value;
    private Float mox_u_235_sigma;
    private Float mox_u_236_value;
    private Float mox_u_236_sigma;
    private Float mox_u_238_value;
    private Float mox_u_238_sigma;
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

    public void setPu_240eff_value(Float pu_240eff_value) {
        this.pu_240eff_value = pu_240eff_value;
    }

    public Float getPu_240eff_sigma() {
        return pu_240eff_sigma;
    }

    public void setPu_240eff_sigma(Float pu_240eff_sigma) {
        this.pu_240eff_sigma = pu_240eff_sigma;
    }

    public Integer getCertificate_mox_id() {
        return certificate_mox_id;
    }

    public void setCertificate_mox_id(Integer certificate_mox_id) {
        this.certificate_mox_id = certificate_mox_id;
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

    public void setReference_date(final String reference_date) {
        this.reference_date = reference_date;
    }

    public Float getPu238_value() {
        return pu238_value;
    }

    public void setPu238_value(Float pu238_value) {
        this.pu238_value = pu238_value;
    }

    public Float getPu238_sigma() {
        return pu238_sigma;
    }

    public void setPu238_sigma(Float pu238_sigma) {
        this.pu238_sigma = pu238_sigma;
    }

    public Float getPu239_value() {
        return pu239_value;
    }

    public void setPu239_value(Float pu239_value) {
        this.pu239_value = pu239_value;
    }

    public Float getPu239_sigma() {
        return pu239_sigma;
    }

    public void setPu239_sigma(Float pu239_sigma) {
        this.pu239_sigma = pu239_sigma;
    }

    public Float getPu240_value() {
        return pu240_value;
    }

    public void setPu240_value(Float pu240_value) {
        this.pu240_value = pu240_value;
    }

    public Float getPu240_sigma() {
        return pu240_sigma;
    }

    public void setPu240_sigma(Float pu240_sigma) {
        this.pu240_sigma = pu240_sigma;
    }

    public Float getPu241_value() {
        return pu241_value;
    }

    public void setPu241_value(Float pu241_value) {
        this.pu241_value = pu241_value;
    }

    public Float getPu241_sigma() {
        return pu241_sigma;
    }

    public void setPu241_sigma(Float pu241_sigma) {
        this.pu241_sigma = pu241_sigma;
    }

    public Float getPu242_value() {
        return pu242_value;
    }

    public void setPu242_value(Float pu242_value) {
        this.pu242_value = pu242_value;
    }

    public Float getPu242_sigma() {
        return pu242_sigma;
    }

    public void setPu242_sigma(Float pu242_sigma) {
        this.pu242_sigma = pu242_sigma;
    }

    public Float getAm241_value() {
        return am241_value;
    }

    public void setAm241_value(Float am241_value) {
        this.am241_value = am241_value;
    }

    public Float getAm241_sigma() {
        return am241_sigma;
    }

    public void setAm241_sigma(Float am241_sigma) {
        this.am241_sigma = am241_sigma;
    }

    public Float getMox_u_percent() {
        return mox_u_percent;
    }

    public void setMox_u_percent(Float mox_u_percent) {
        this.mox_u_percent = mox_u_percent;
    }

    public Float getMox_pu_percent() {
        return mox_pu_percent;
    }

    public void setMox_pu_percent(Float mox_pu_percent) {
        this.mox_pu_percent = mox_pu_percent;
    }

    public Float getMox_u_pu_ratio_value() {
        return mox_u_pu_ratio_value;
    }

    public void setMox_u_pu_ratio_value(Float mox_u_pu_ratio_value) {
        this.mox_u_pu_ratio_value = mox_u_pu_ratio_value;
    }

    public Float getMox_u_pu_ratio_sigma() {
        return mox_u_pu_ratio_sigma;
    }

    public void setMox_u_pu_ratio_sigma(Float mox_u_pu_ratio_sigma) {
        this.mox_u_pu_ratio_sigma = mox_u_pu_ratio_sigma;
    }

    public Float getMox_u_234_value() {
        return mox_u_234_value;
    }

    public void setMox_u_234_value(Float mox_u_234_value) {
        this.mox_u_234_value = mox_u_234_value;
    }

    public Float getMox_u_234_sigma() {
        return mox_u_234_sigma;
    }

    public void setMox_u_234_sigma(Float mox_u_234_sigma) {
        this.mox_u_234_sigma = mox_u_234_sigma;
    }

    public Float getMox_u_235_value() {
        return mox_u_235_value;
    }

    public void setMox_u_235_value(Float mox_u_235_value) {
        this.mox_u_235_value = mox_u_235_value;
    }

    public Float getMox_u_235_sigma() {
        return mox_u_235_sigma;
    }

    public void setMox_u_235_sigma(Float mox_u_235_sigma) {
        this.mox_u_235_sigma = mox_u_235_sigma;
    }

    public Float getMox_u_236_value() {
        return mox_u_236_value;
    }

    public void setMox_u_236_value(Float mox_u_236_value) {
        this.mox_u_236_value = mox_u_236_value;
    }

    public Float getMox_u_236_sigma() {
        return mox_u_236_sigma;
    }

    public void setMox_u_236_sigma(Float mox_u_236_sigma) {
        this.mox_u_236_sigma = mox_u_236_sigma;
    }

    public Float getMox_u_238_value() {
        return mox_u_238_value;
    }

    public void setMox_u_238_value(Float mox_u_238_value) {
        this.mox_u_238_value = mox_u_238_value;
    }

    public Float getMox_u_238_sigma() {
        return mox_u_238_sigma;
    }

    public void setMox_u_238_sigma(Float mox_u_238_sigma) {
        this.mox_u_238_sigma = mox_u_238_sigma;
    }

    public Float getPu_mass_value() {
        return pu_mass_value;
    }

    public void setPu_mass_value(Float pu_mass_value) {
        this.pu_mass_value = pu_mass_value;
    }

    public Float getPu_mass_sigma() {
        return pu_mass_sigma;
    }

    public void setPu_mass_sigma(Float pu_mass_sigma) {
        this.pu_mass_sigma = pu_mass_sigma;
    }

    public Float getPu_content_value() {
        return pu_content_value;
    }

    public void setPu_content_value(Float pu_content_value) {
        this.pu_content_value = pu_content_value;
    }

    public Float getPu_content_sigma() {
        return pu_content_sigma;
    }

    public void setPu_content_sigma(Float pu_content_sigma) {
        this.pu_content_sigma = pu_content_sigma;
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

    final private static String NAME_VALUE_PAIR_MOX_U_PERCENT = "certificate.mox.u.percent";
    final private static String NAME_VALUE_PAIR_MOX_PU_PERCENT = "certificate.mox.pu.percent";
    final private static String NAME_VALUE_PAIR_MOX_U_PU_RATIO_VALUE = "certificate.mox.u_pu_ratio.value";
    final private static String NAME_VALUE_PAIR_MOX_U_PU_RATIO_SIGMA = "certificate.mox.u_pu_ratio.sigma";

    final private static String NAME_VALUE_PAIR_MOX_U_234_VALUE = "certificate.mox.u.234.value";
    final private static String NAME_VALUE_PAIR_MOX_U_234_SIGMA = "certificate.mox.u.234.sigma";
    final private static String NAME_VALUE_PAIR_MOX_U_235_VALUE = "certificate.mox.u.235.value";
    final private static String NAME_VALUE_PAIR_MOX_U_235_SIGMA = "certificate.mox.u.235.sigma";
    final private static String NAME_VALUE_PAIR_MOX_U_236_VALUE = "certificate.mox.u.236.value";
    final private static String NAME_VALUE_PAIR_MOX_U_236_SIGMA = "certificate.mox.u.236.sigma";
    final private static String NAME_VALUE_PAIR_MOX_U_238_VALUE = "certificate.mox.u.238.value";
    final private static String NAME_VALUE_PAIR_MOX_U_238_SIGMA = "certificate.mox.u.238.sigma";

    final private static String NAME_VALUE_PAIR_PU_MASS_VALUE = "supplementary.pu.mass.value";
    final private static String NAME_VALUE_PAIR_PU_MASS_SIGMA = "supplementary.pu.mass.sigma";
    final private static String NAME_VALUE_PAIR_PU_CONTENT_VALUE = "supplementary.pu.content.value";
    final private static String NAME_VALUE_PAIR_PU_CONTENT_SIGMA = "supplementary.pu.content.sigma";

    final private static String NAME_VALUE_PAIR_PU_240EFF_VALUE = "certificate.pu.240eff.value";
    final private static String NAME_VALUE_PAIR_PU_240EFF_SIGMA = "certificate.pu.240eff.sigma";

    final private static String NAME_VALUE_PAIR_SEPARATION_DATE = "certificate.separation_date";
    // The plutonium data has a 1-to-1 with the certificate, so cache the plutonimum
    // enity using the certificate id
    final private static Map<Integer, CertificateMOXEntity> cache = new HashMap<>();
    final private static String INSERT_CERTIFICATE_MOX= " INSERT INTO certificate_mox_table( "
            + " certificate_id, reference_date,  "
            + " pu_238_value, pu_238_sigma, pu_239_value, pu_239_sigma, "
            + " pu_240_value, pu_240_sigma, pu_241_value, pu_241_sigma, "
            + " pu_242_value, pu_242_sigma, am_241_value, am_241_sigma, "
            + " mox_u_percent, mox_pu_percent, "
            + " mox_u_pu_ratio_value, mox_u_pu_ratio_sigma, "
            + " mox_u_234_value, mox_u_234_sigma, mox_u_235_value, mox_u_235_sigma, "
            + " mox_u_236_value, mox_u_236_sigma, mox_u_238_value, mox_u_238_sigma, "
            + " pu_mass_value, pu_mass_sigma, pu_content_value, pu_content_sigma, "
            + " pu_240eff_value, pu_240eff_sigma, separation_date  ) "
            + "  VALUES( ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, "
            + "          ?,?,?,?,?, ?,?,?,?,?, ?,?,? ); ";

    public static final CertificateMOXEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final CertificateMOXEntity entity = CertificateMOXEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Not enough information to make certificate_mox: " + nameValuePairs);
        }
        if (entity.getCertificate_mox_id() != null) {
            return entity;
        }
        
        try (final PreparedStatement statement = conn.prepareStatement(
                CertificateMOXEntity.INSERT_CERTIFICATE_MOX,
                Statement.RETURN_GENERATED_KEYS)) {
            setStatementValues(statement, entity); 
            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setCertificate_mox_id(key);;
                    CertificateMOXEntity.cache.put(entity.getCertificate_mox_id(), entity);
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
            final CertificateMOXEntity entity) {
        try {
            statement.setInt(1, entity.getCertificateEntity().getCertificateId());
            setDatetime(2, statement, entity.getReference_date());
            setFloat(3, statement, entity.getPu238_value());
            setFloat(4, statement, entity.getPu238_sigma());
            setFloat(5, statement, entity.getPu239_value());
            setFloat(6, statement, entity.getPu239_sigma());
            setFloat(7, statement, entity.getPu240_value());
            setFloat(8, statement, entity.getPu240_sigma());
            setFloat(9, statement, entity.getPu241_value());
            setFloat(10, statement, entity.getPu241_sigma());
            setFloat(11, statement, entity.getPu242_value());
            setFloat(12, statement, entity.getPu242_sigma());
            setFloat(13, statement, entity.getAm241_value());
            setFloat(14, statement, entity.getAm241_sigma());
            setFloat(15, statement, entity.getMox_u_percent());
            setFloat(16, statement, entity.getMox_pu_percent());
            setFloat(17, statement, entity.getMox_u_pu_ratio_value());
            setFloat(18, statement, entity.getMox_u_pu_ratio_sigma());
            setFloat(19, statement, entity.getMox_u_234_value());
            setFloat(20, statement, entity.getMox_u_234_sigma());
            setFloat(21, statement, entity.getMox_u_235_value());
            setFloat(22, statement, entity.getMox_u_235_sigma());
            setFloat(23, statement, entity.getMox_u_236_value());
            setFloat(24, statement, entity.getMox_u_236_sigma());
            setFloat(25, statement, entity.getMox_u_238_value());
            setFloat(26, statement, entity.getMox_u_238_sigma());
            setFloat(27, statement, entity.getPu_mass_value());
            setFloat(28, statement, entity.getPu_mass_sigma());
            setFloat(29, statement, entity.getPu_content_value());
            setFloat(30, statement, entity.getPu_content_sigma());
            setFloat(31, statement, entity.getPu_240eff_value());
            setFloat(32, statement, entity.getPu_240eff_sigma());
            setDatetime(33, statement, entity.getSeparation_date());
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

    private static CertificateMOXEntity makeEntity(final Connection conn, final Map<String, String> nameValuePairs) {
        final CertificateMOXEntity entity = new CertificateMOXEntity();
        entity.setCertificateEntity(CertificateEntity.load(conn, nameValuePairs));
        entity.setReference_date(getDatetimeValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_REFERENCE_DATE)));
        entity.setPu238_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_238_VALUE)));
        entity.setPu238_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_238_SIGMA)));
        entity.setPu239_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_239_VALUE)));
        entity.setPu239_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_239_SIGMA)));
        entity.setPu240_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_240_VALUE)));
        entity.setPu240_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_240_SIGMA)));
        entity.setPu241_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_241_VALUE)));
        entity.setPu241_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_241_SIGMA)));
        entity.setPu242_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_242_VALUE)));
        entity.setPu242_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_242_SIGMA)));
        entity.setAm241_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_AM_241_VALUE)));
        entity.setAm241_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_AM_241_SIGMA)));
        entity.setMox_u_percent(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_PERCENT)));
        entity.setMox_pu_percent(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_PU_PERCENT)));
        entity.setMox_u_pu_ratio_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_PU_RATIO_VALUE)));
        entity.setMox_u_pu_ratio_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_PU_RATIO_SIGMA)));
        entity.setMox_u_234_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_234_VALUE)));
        entity.setMox_u_234_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_234_SIGMA)));
        entity.setMox_u_235_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_235_VALUE)));
        entity.setMox_u_235_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_235_SIGMA)));
        entity.setMox_u_236_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_236_VALUE)));
        entity.setMox_u_236_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_236_SIGMA)));
        entity.setMox_u_238_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_238_VALUE)));
        entity.setMox_u_238_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_MOX_U_238_SIGMA)));
        entity.setPu_mass_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_MASS_VALUE)));
        entity.setPu_mass_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_MASS_SIGMA)));
        entity.setPu_content_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_CONTENT_VALUE)));
        entity.setPu_content_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_CONTENT_SIGMA)));
        entity.setPu_240eff_value(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_240EFF_VALUE)));
        entity.setPu_240eff_sigma(getFloatValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_PU_240EFF_SIGMA)));
        entity.setSeparation_date(getDatetimeValue(nameValuePairs.get(CertificateMOXEntity.NAME_VALUE_PAIR_SEPARATION_DATE)));

        return entity;
    }

    public static final CertificateMOXEntity find(final Connection conn, final Map<String, String> nameValuePairs) {
        
        final CertificateMOXEntity entity = CertificateMOXEntity.makeEntity(conn, nameValuePairs);
        
        for (final CertificateMOXEntity cachedEntity : CertificateMOXEntity.cache.values()) {
            if (CertificateMOXEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        return entity;
    }
    
    public static final boolean haveEqualValues(final CertificateMOXEntity first,
                                                final CertificateMOXEntity second) {
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

        if (first.getAm241_sigma() == null) {
            if (second.getAm241_sigma() != null) {
                return false;
            }
        } else if (!first.getAm241_sigma().equals(second.getAm241_sigma())) {
            return false;
        }
        
        if (first.getAm241_value() == null) {
            if (second.getAm241_value() != null) {
                return false;
            }
        } else if (!first.getAm241_value().equals(second.getAm241_value())) {
            return false;
        }
        
        if (first.getMox_pu_percent() == null) {
            if (second.getMox_pu_percent() != null) {
                return false;
            }
        } else if (!first.getMox_pu_percent().equals(second.getMox_pu_percent())) {
            return false;
        }
        
        if (first.getMox_u_234_value() == null) {
            if (second.getMox_u_234_value() != null) {
                return false;
            }
        } else if (!first.getMox_u_234_value().equals(second.getMox_u_234_value())) {
            return false;
        }
        
        if (first.getMox_u_234_sigma() == null) {
            if (second.getMox_u_234_sigma() != null) {
                return false;
            }
        } else if (!first.getMox_u_234_sigma().equals(second.getMox_u_234_sigma())) {
            return false;
        }
        
        if (first.getMox_u_235_value() == null) {
            if (second.getMox_u_235_value() != null) {
                return false;
            }
        } else if (!first.getMox_u_235_value().equals(second.getMox_u_235_value())) {
            return false;
        }
        
        if (first.getMox_u_235_sigma() == null) {
            if (second.getMox_u_235_sigma() != null) {
                return false;
            }
        } else if (!first.getMox_u_235_sigma().equals(second.getMox_u_235_sigma())) {
            return false;
        }
        
        if (first.getMox_u_236_value() == null) {
            if (second.getMox_u_236_value() != null) {
                return false;
            }
        } else if (!first.getMox_u_236_value().equals(second.getMox_u_236_value())) {
            return false;
        }
        
        if (first.getMox_u_236_sigma() == null) {
            if (second.getMox_u_236_sigma() != null) {
                return false;
            }
        } else if (!first.getMox_u_236_sigma().equals(second.getMox_u_236_sigma())) {
            return false;
        }
        
        if (first.getMox_u_238_value() == null) {
            if (second.getMox_u_238_value() != null) {
                return false;
            }
        } else if (!first.getMox_u_238_value().equals(second.getMox_u_238_value())) {
            return false;
        }
        
        if (first.getMox_u_238_sigma() == null) {
            if (second.getMox_u_238_sigma() != null) {
                return false;
            }
        } else if (!first.getMox_u_238_sigma().equals(second.getMox_u_238_sigma())) {
            return false;
        }
        
        if (first.getMox_u_percent() == null) {
            if (second.getMox_u_percent() != null) {
                return false;
            }
        } else if (!first.getMox_u_percent().equals(second.getMox_u_percent())) {
            return false;
        }
        
        if (first.getMox_u_pu_ratio_sigma() == null) {
            if (second.getMox_u_pu_ratio_sigma() != null) {
                return false;
            }
        } else if (!first.getMox_u_pu_ratio_sigma().equals(second.getMox_u_pu_ratio_sigma())) {
            return false;
        }
        
        if (first.getMox_u_pu_ratio_value() == null) {
            if (second.getMox_u_pu_ratio_value() != null) {
                return false;
            }
        } else if (!first.getMox_u_pu_ratio_value().equals(second.getMox_u_pu_ratio_value())) {
            return false;
        }
        
        if (first.getPu238_sigma() == null) {
            if (second.getPu238_sigma() != null) {
                return false;
            }
        } else if (!first.getPu238_sigma().equals(second.getPu238_sigma())) {
            return false;
        }
        
        if (first.getPu238_value() == null) {
            if (second.getPu238_value() != null) {
                return false;
            }
        } else if (!first.getPu238_value().equals(second.getPu238_value())) {
            return false;
        }
        
        if (first.getPu239_sigma() == null) {
            if (second.getPu239_sigma() != null) {
                return false;
            }
        } else if (!first.getPu239_sigma().equals(second.getPu239_sigma())) {
            return false;
        }
        
        if (first.getPu239_value() == null) {
            if (second.getPu239_value() != null) {
                return false;
            }
        } else if (!first.getPu239_value().equals(second.getPu239_value())) {
            return false;
        }
        
        if (first.getPu240_sigma() == null) {
            if (second.getPu240_sigma() != null) {
                return false;
            }
        } else if (!first.getPu240_sigma().equals(second.getPu240_sigma())) {
            return false;
        }
        
        if (first.getPu240_value() == null) {
            if (second.getPu240_value() != null) {
                return false;
            }
        } else if (!first.getPu240_value().equals(second.getPu240_value())) {
            return false;
        }
        
        if (first.getPu241_sigma() == null) {
            if (second.getPu241_sigma() != null) {
                return false;
            }
        } else if (!first.getPu241_sigma().equals(second.getPu241_sigma())) {
            return false;
        }
        
        if (first.getPu241_value() == null) {
            if (second.getPu241_value() != null) {
                return false;
            }
        } else if (!first.getPu241_value().equals(second.getPu241_value())) {
            return false;
        }
        
        if (first.getPu242_sigma() == null) {
            if (second.getPu242_sigma() != null) {
                return false;
            }
        } else if (!first.getPu242_sigma().equals(second.getPu242_sigma())) {
            return false;
        }
        
        if (first.getPu242_value() == null) {
            if (second.getPu242_value() != null) {
                return false;
            }
        } else if (!first.getPu242_value().equals(second.getPu242_value())) {
            return false;
        }
        
        if (first.getPu_content_sigma() == null) {
            if (second.getPu_content_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_content_sigma().equals(second.getPu_content_sigma())) {
            return false;
        }
        
        if (first.getPu_content_value() == null) {
            if (second.getPu_content_value() != null) {
                return false;
            }
        } else if (!first.getPu_content_value().equals(second.getPu_content_value())) {
            return false;
        }
        
        if (first.getPu_mass_sigma() == null) {
            if (second.getPu_mass_sigma() != null) {
                return false;
            }
        } else if (!first.getPu_mass_sigma().equals(second.getPu_mass_sigma())) {
            return false;
        }
        
        if (first.getPu_mass_value() == null) {
            if (second.getPu_mass_value() != null) {
                return false;
            }
        } else if (!first.getPu_mass_value().equals(second.getPu_mass_value())) {
            return false;
        }
        
        if (first.getReference_date() == null) {
            if (second.getReference_date() != null) {
                return false;
            }
        } else if (!first.getReference_date().equals(second.getReference_date())) {
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

//    private static Integer getIntegerValue(final String value) {
//        if (value == null) {
//            return null;
//        } else {
//            return Integer.valueOf(value) < 0 ? null : Integer.valueOf(value);
//        }
//    }

    public static String getDatetimeValue(final String value) {
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
