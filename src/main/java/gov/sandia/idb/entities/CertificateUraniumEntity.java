package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class CertificateUraniumEntity {

    private Integer certificateUraniumId;
    private CertificateEntity certificateEntity;
    private String referenceDate;
    private String separationDate;
    private Float u234Value;
    private Float u234Sigma;
    private Float u235Value;
    private Float u235Sigma;
    private Float u236Sigma;
    private Float u236Value;
    private Float u238Sigma;
    private Float u238Value;

    public Integer getCertificateUraniumId() {
        return certificateUraniumId;
    }

    public void setCertificateUraniumId(Integer certificateUraniumId) {
        this.certificateUraniumId = certificateUraniumId;
    }

    public CertificateEntity getCertificateEntity() {
        return certificateEntity;
    }

    public void setCertificateEntity(CertificateEntity certificateEntity) {
        this.certificateEntity = certificateEntity;
    }

    public String getReferenceDate() {
        return referenceDate;
    }

    public void setReferenceDate(final String referenceDate) {
        this.referenceDate = referenceDate;
    }

    public String getSeparationDate() {
        return separationDate;
    }

    public void setSeparationDate(final String separationDate) {
        this.separationDate = separationDate;
    }

    public Float getU234Value() {
        return u234Value;
    }

    public void setU234Value(Float u234Value) {
        this.u234Value = u234Value;
    }

    public Float getU234Sigma() {
        return u234Sigma;
    }

    public void setU234Sigma(Float u234Sigma) {
        this.u234Sigma = u234Sigma;
    }

    public Float getU235Value() {
        return u235Value;
    }

    public void setU235Value(Float u235Value) {
        this.u235Value = u235Value;
    }

    public Float getU235Sigma() {
        return u235Sigma;
    }

    public void setU235Sigma(Float u235Sigma) {
        this.u235Sigma = u235Sigma;
    }

    public Float getU236Sigma() {
        return u236Sigma;
    }

    public void setU236Sigma(Float u236Sigma) {
        this.u236Sigma = u236Sigma;
    }

    public Float getU236Value() {
        return u236Value;
    }

    public void setU236Value(Float u236Value) {
        this.u236Value = u236Value;
    }

    public Float getU238Sigma() {
        return u238Sigma;
    }

    public void setU238Sigma(Float u238Sigma) {
        this.u238Sigma = u238Sigma;
    }

    public Float getU238Value() {
        return u238Value;
    }

    public void setU238Value(Float u238Value) {
        this.u238Value = u238Value;
    }

    @Override
    public String toString() {
        return "CertificateUraniumEntity [certificateUraniumId=" + certificateUraniumId + ", certificateEntity="
                + certificateEntity + ", referenceDate=" + referenceDate + ", separationDate=" + separationDate
                + ", u234Value=" + u234Value + ", u234Sigma=" + u234Sigma + ", u235Value=" + u235Value + ", u235Sigma="
                + u235Sigma + ", u236Sigma=" + u236Sigma + ", u236Value=" + u236Value + ", u238Sigma=" + u238Sigma
                + ", u238Value=" + u238Value + "]";
    }

    final private static String NAME_VALUE_PAIR_REFERENCE_DATE = "certificate.u.reference_date";
    final private static String NAME_VALUE_PAIR_SEPARATION_DATE = "certificate.separation_date";

    final private static String NAME_VALUE_PAIR_U_234_VALUE = "certificate.u.u.234.value";
    final private static String NAME_VALUE_PAIR_U_234_SIGMA = "certificate.u.u.234.sigma";
    final private static String NAME_VALUE_PAIR_U_235_VALUE = "certificate.u.u.235.value";
    final private static String NAME_VALUE_PAIR_U_235_SIGMA = "certificate.u.u.235.sigma";
    final private static String NAME_VALUE_PAIR_U_236_VALUE = "certificate.u.u.236.value";
    final private static String NAME_VALUE_PAIR_U_236_SIGMA = "certificate.u.u.236.sigma";
    final private static String NAME_VALUE_PAIR_U_238_VALUE = "certificate.u.u.238.value";
    final private static String NAME_VALUE_PAIR_U_238_SIGMA = "certificate.u.u.238.sigma";

    final private static Map<Integer, CertificateUraniumEntity> cache = new HashMap<>();
    private static final String INSERT_CERTIFICATE_URANIUM = " INSERT INTO certificate_u_table ( "
            + "   certificate_id, reference_date, "
            + "   separation_date, u_234_value, u_234_sigma, "
            + "   u_235_value, u_235_sigma, u_236_value, u_236_sigma, "
            + "   u_238_value, u_238_sigma ) "
            + " VALUES ( ?,?,?,?,?, ?,?,?,?,?, ? ); ";

    public static CertificateUraniumEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        CertificateUraniumEntity entity = CertificateUraniumEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make a certificate_u: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getCertificateUraniumId() != null) {
            return entity;
        }

        // need to create a new entity...
        try (final PreparedStatement statement = conn.prepareStatement(
                CertificateUraniumEntity.INSERT_CERTIFICATE_URANIUM,
                Statement.RETURN_GENERATED_KEYS)) {

            // there should never be a uranium certificate without a certificate
            // association...
            statement.setInt(1, entity.getCertificateEntity().getCertificateId());

            CertificateUraniumEntity.setDatetime(2, statement, entity.getReferenceDate());
            CertificateUraniumEntity.setDatetime(3, statement, entity.getSeparationDate());

            CertificateUraniumEntity.setFloat(4, statement, entity.getU234Value());
            CertificateUraniumEntity.setFloat(5, statement, entity.getU234Sigma());
            CertificateUraniumEntity.setFloat(6, statement, entity.getU235Value());
            CertificateUraniumEntity.setFloat(7, statement, entity.getU235Sigma());
            CertificateUraniumEntity.setFloat(8, statement, entity.getU236Value());
            CertificateUraniumEntity.setFloat(9, statement, entity.getU236Sigma());
            CertificateUraniumEntity.setFloat(10, statement, entity.getU238Value());
            CertificateUraniumEntity.setFloat(11, statement, entity.getU238Sigma());

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setCertificateUraniumId(key);

                    // Note that the key is the uid spectrum so that
                    // the spectrum counts and spectrum md5 entities
                    // can find the spectrum and its database id...
                    CertificateUraniumEntity.cache.put(entity.getCertificateUraniumId(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting supplementary measurement values from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    private static final CertificateUraniumEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String referenceDateString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_REFERENCE_DATE);
        final String separationDateString = nameValuePairs
                .get(CertificateUraniumEntity.NAME_VALUE_PAIR_SEPARATION_DATE);

        final String u234SigmaString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_234_SIGMA);
        final String u234ValueString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_234_VALUE);
        final String u235SigmaString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_235_SIGMA);
        final String u235ValueString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_235_VALUE);
        final String u236SigmaString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_236_SIGMA);
        final String u236ValueString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_236_VALUE);
        final String u238SigmaString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_238_SIGMA);
        final String u238ValueString = nameValuePairs.get(CertificateUraniumEntity.NAME_VALUE_PAIR_U_238_VALUE);

        final CertificateUraniumEntity entity = new CertificateUraniumEntity();

        entity.setCertificateEntity(CertificateEntity.load(conn, nameValuePairs));

        entity.setReferenceDate(CertificateUraniumEntity.getDatetimeValue(referenceDateString));
        entity.setSeparationDate(CertificateUraniumEntity.getDatetimeValue(separationDateString));

        entity.setU234Sigma(getValueFromString(u234SigmaString));
        entity.setU234Value(getValueFromString(u234ValueString));
        entity.setU235Sigma(getValueFromString(u235SigmaString));
        entity.setU235Value(getValueFromString(u235ValueString));
        entity.setU236Sigma(getValueFromString(u236SigmaString));
        entity.setU236Value(getValueFromString(u236ValueString));
        entity.setU238Sigma(getValueFromString(u238SigmaString));
        entity.setU238Value(getValueFromString(u238ValueString));

        for (final CertificateUraniumEntity cachedEntity : CertificateUraniumEntity.cache.values()) {
            if (CertificateUraniumEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // this entity has new values that need to be added to the
        // table, so return the non-cached entity...
        return entity;
    }

    private static Float getValueFromString(final String stringValue) {
        return stringValue == null ? null : Float.valueOf(stringValue);
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

    private static void setDatetime(final int position,
            final PreparedStatement statement,
            final String datetimeString) throws SQLException {
        if (datetimeString == null) {
            statement.setNull(position, Types.TIMESTAMP);
        } else {
            statement.setString(position, datetimeString);
        }
    }

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

    public static boolean haveEqualValues(final CertificateUraniumEntity first,
            final CertificateUraniumEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
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

        if (first.getReferenceDate() == null) {
            if (second.getReferenceDate() != null) {
                return false;
            }
        } else if (!first.getReferenceDate().equals(second.getReferenceDate())) {
            return false;
        }

        if (first.getSeparationDate() == null) {
            if (second.getSeparationDate() != null) {
                return false;
            }
        } else if (!first.getSeparationDate().equals(second.getSeparationDate())) {
            return false;
        }

        if (first.getU234Sigma() == null) {
            if (second.getU234Sigma() != null) {
                return false;
            }
        } else if (!first.getU234Sigma().equals(second.getU234Sigma())) {
            return false;
        }

        if (first.getU234Value() == null) {
            if (second.getU234Value() != null) {
                return false;
            }
        } else if (!first.getU234Value().equals(second.getU234Value())) {
            return false;
        }

        if (first.getU235Sigma() == null) {
            if (second.getU235Sigma() != null) {
                return false;
            }
        } else if (!first.getU235Sigma().equals(second.getU235Sigma())) {
            return false;
        }

        if (first.getU235Value() == null) {
            if (second.getU235Value() != null) {
                return false;
            }
        } else if (!first.getU235Value().equals(second.getU235Value())) {
            return false;
        }

        if (first.getU236Sigma() == null) {
            if (second.getU236Sigma() != null) {
                return false;
            }
        } else if (!first.getU236Sigma().equals(second.getU236Sigma())) {
            return false;
        }

        if (first.getU236Value() == null) {
            if (second.getU236Value() != null) {
                return false;
            }
        } else if (!first.getU236Value().equals(second.getU236Value())) {
            return false;
        }

        if (first.getU238Sigma() == null) {
            if (second.getU238Sigma() != null) {
                return false;
            }
        } else if (!first.getU238Sigma().equals(second.getU238Sigma())) {
            return false;
        }

        if (first.getU238Value() == null) {
            if (second.getU238Value() != null) {
                return false;
            }
        } else if (!first.getU238Value().equals(second.getU238Value())) {
            return false;
        }

        return true;
    }
}
