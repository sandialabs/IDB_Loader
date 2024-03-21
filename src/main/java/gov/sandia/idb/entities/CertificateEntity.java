package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class CertificateEntity {
    private Integer CertificateId;
    private Integer uidSource;
    private LoadHistoryEntity loadHistoryEntity;
    private String CertificateIdentification;
    private SourceTypeEntity sourceTypeEntity;
    private String certificateDocument;
    private String fractionUnit;
    private String uncertainty;
    private String uncertaintyNumberOfSigma;

    public CertificateEntity() {

    }

    public Integer getCertificateId() {
        return CertificateId;
    }

    public void setCertificateId(Integer certificateId) {
        CertificateId = certificateId;
    }

    public String getCertificateIdentification() {
        return CertificateIdentification;
    }

    public void setCertificateIdentification(String certificateIdentification) {
        CertificateIdentification = certificateIdentification;
    }

    public SourceTypeEntity getSourceTypeEntity() {
        return sourceTypeEntity;
    }

    public void setSourceTypeEntity(SourceTypeEntity sourceTypeEntity) {
        this.sourceTypeEntity = sourceTypeEntity;
    }

    public Integer getUidSource() {
        return uidSource;
    }

    public void setUidSource(Integer uidSource) {
        this.uidSource = uidSource;
    }

    public String getCertificateDocument() {
        return certificateDocument;
    }

    public void setCertificateDocument(String certificateDocument) {
        this.certificateDocument = certificateDocument;
    }

    public LoadHistoryEntity getLoadHistoryEntity() {
        return this.loadHistoryEntity;
    }

    public void setLoadHistoryEntity(LoadHistoryEntity loadHistoryEntity) {
        this.loadHistoryEntity = loadHistoryEntity;
    }

    public String getFractionUnit() {
        return fractionUnit;
    }

    public void setFractionUnit(String fractionUnit) {
        this.fractionUnit = fractionUnit;
    }

    public String getUncertainty() {
        return uncertainty;
    }

    public void setUncertainty(String uncertainty) {
        this.uncertainty = uncertainty;
    }

    public String getUncertaintyNumberOfSigma() {
        return uncertaintyNumberOfSigma;
    }

    public void setUncertaintyNumberOfSigma(String uncertaintyNumberOfSigma) {
        this.uncertaintyNumberOfSigma = uncertaintyNumberOfSigma;
    }

    @Override
    public String toString() {
        return "CertificateEntity [CertificateId=" + CertificateId + ", uidSource=" + uidSource + ", loadHistoryEntity="
                + loadHistoryEntity + ", CertificateIdentification=" + CertificateIdentification + ", sourceTypeEntity="
                + sourceTypeEntity + ", certificateDocument=" + certificateDocument + ", fractionUnit=" + fractionUnit
                + ", uncertainty=" + uncertainty + ", uncertaintyNumberOfSigma=" + uncertaintyNumberOfSigma + "]";
    }

    final private static String NAME_VALUE_PAIR_CERTIFICATE_IDENTIFICATION = "certificate.identification";
    final private static String NAME_VALUE_PAIR_CERTIFICATE_UID_SOURCE = "UID.source";
    final private static String NAME_VALUE_PAIR_CERTIFICATE_DOCUMENT = "certificate.document";
    final private static String NAME_VALUE_PAIR_CERTIFICATE_FRACTION_UNIT = "certificate.fraction_unit";
    final private static String NAME_VALUE_PAIR_CERTIFICATE_UNCERTAINTY = "certificate.uncertainty";
    final private static String NAME_VALUE_PAIR_CERTIFICATE_UNCERTAINTY_NUMBER_OF_SIGMA = "certificate.uncertainty.number_of_sigma";

    final private static Map<Integer, CertificateEntity> cache = new HashMap<>();
    final private static String INSERT_CERTIFICATE = " INSERT INTO certificate_table( "
            + "   uid_source, certificate_identification, source_type_id, "
            + "   load_history_id, certificate_document, "
            + "   fraction_unit, uncertainty, uncertainty_number_of_sigma ) "
            + "  VALUES( ?,?,?,?,?, ?,?,? );";
    final private static String SELECT_CERTIFICATE = " SELECT certificate_id, "
            + "   uid_source, certificate_identification, source_type_id, "
            + "   load_history_id, certificate_document, "
            + "   fraction_unit, uncertainty, uncertainty_number_of_sigma "
            + " FROM certificate_table "
            + " WHERE uid_source = (?) "
            + "   AND certificate_identification <=> (?) "
            + "   AND source_type_id = (?) "
            + "   AND load_history_id = (?) "
            + "   AND certificate_document <=> (?) "
            + "   AND fraction_unit <=> (?) "
            + "   AND uncertainty <=> (?) "
            + "   AND uncertainty_number_of_sigma <=> (?); ";

    public static final CertificateEntity load(final Connection conn, final Map<String, String> nameValuePairs) {

        final CertificateEntity entity = CertificateEntity.find(conn, nameValuePairs);
        if (entity == null) {
            throw new RuntimeException("Not enough information to make Certificate Entity. " + nameValuePairs);
        }
        if (entity.getCertificateId() != null) {
            return entity;
        }

        try (final PreparedStatement statement = conn.prepareStatement(CertificateEntity.INSERT_CERTIFICATE,
                Statement.RETURN_GENERATED_KEYS)) {
            statement.setInt(1, entity.getUidSource());
            setStatementStringValue(statement, entity.getCertificateIdentification(), 2);
            statement.setInt(3, entity.getSourceTypeEntity().getSourceTypeId());
            statement.setInt(4, entity.getLoadHistoryEntity().getLoadHistoryId());
            setStatementStringValue(statement, entity.getCertificateDocument(), 5);
            setStatementStringValue(statement, entity.getFractionUnit(), 6);
            setStatementStringValue(statement, entity.getUncertainty(), 7);
            setStatementStringValue(statement, entity.getUncertaintyNumberOfSigma(), 8);

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setCertificateId(key);
                }
            }
            CertificateEntity.cache.put(entity.getUidSource(), entity);
            if (entity.getSourceTypeEntity().getSourceTypeName().equals("PU")) {
                CertificatePlutoniumEntity.load(conn, nameValuePairs);
            } else if (entity.getSourceTypeEntity().getSourceTypeName().equals("MOX")) {
                CertificateMOXEntity.load(conn, nameValuePairs);
            } else if (entity.getSourceTypeEntity().getSourceTypeName().equals("U")) {
                CertificateUraniumEntity.load(conn, nameValuePairs);
            }
            return entity;
        } catch (final Exception e) {
            throw new RuntimeException("Inserting certificate identificate from pairs: " + nameValuePairs, e);
        }
    }

    public static final CertificateEntity find(final String inputUIDSourceString) {
        if (inputUIDSourceString == null) {
            return null;
        }
        return CertificateEntity.cache.get(Integer.valueOf(inputUIDSourceString.trim()));
    }

    private static final CertificateEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String certificateIdentification = CertificateEntity.getStringValue(
                CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_IDENTIFICATION,
                nameValuePairs);
        final String certificateDocument = CertificateEntity.getStringValue(
                CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_DOCUMENT,
                nameValuePairs);
        final String fractionUnit = CertificateEntity.getStringValue(
                CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_FRACTION_UNIT,
                nameValuePairs);
        final String uncertainty = CertificateEntity.getStringValue(
                CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_UNCERTAINTY,
                nameValuePairs);
        final String uncertaintyNumberOfSigma = CertificateEntity.getStringValue(
                CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_UNCERTAINTY_NUMBER_OF_SIGMA,
                nameValuePairs);
        String uidSourceString = nameValuePairs.get(CertificateEntity.NAME_VALUE_PAIR_CERTIFICATE_UID_SOURCE).trim();
        final Integer uidSource = Integer.valueOf(uidSourceString);

        final CertificateEntity entity = new CertificateEntity();
        entity.setCertificateIdentification(certificateIdentification);
        entity.setUidSource(uidSource);
        entity.setLoadHistoryEntity(LoadHistoryEntity.load(conn, nameValuePairs));
        entity.setCertificateDocument(certificateDocument);
        entity.setSourceTypeEntity(SourceTypeEntity.load(conn, nameValuePairs));
        entity.setFractionUnit(fractionUnit);
        entity.setUncertainty(uncertainty);
        entity.setUncertaintyNumberOfSigma(uncertaintyNumberOfSigma);

        for (final CertificateEntity cachedEntity : CertificateEntity.cache.values()) {
            if (CertificateEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // check the database...
        try (final PreparedStatement statement = conn
                .prepareStatement(CertificateEntity.SELECT_CERTIFICATE)) {
            statement.setInt(1, entity.getUidSource());
            setStatementStringValue(statement, entity.getCertificateIdentification(), 2);
            statement.setInt(3, entity.getSourceTypeEntity().getSourceTypeId());
            statement.setInt(4, entity.getLoadHistoryEntity().getLoadHistoryId());
            setStatementStringValue(statement, entity.getCertificateDocument(), 5);
            setStatementStringValue(statement, entity.getFractionUnit(), 6);
            setStatementStringValue(statement, entity.getUncertainty(), 7);
            setStatementStringValue(statement, entity.getUncertaintyNumberOfSigma(), 8);

            try (final ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {

                    // entity in the database has equal values except for id,
                    // so set the database id into entity and return...
                    entity.setCertificateId(rs.getInt("certificate_id"));

                    CertificateEntity.cache.put(entity.getUidSource(), entity);
                    return entity;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Retrieving Certificate: " + entity, e);
        }

        // this certificate entity has new values that need to be added to the
        // measurement table, so return the non-cached, not in database entity...
        return entity;
    }

    private static void setStatementStringValue(final PreparedStatement statement,
            final String string,
            final int parameterIndex) throws SQLException {
        if (string == null || string.isEmpty() || string.trim().length() == 0) {
            statement.setNull(parameterIndex, Types.VARCHAR);
        } else {
            statement.setString(parameterIndex, string);
        }
    }

    private static String getStringValue(final String propertyName, final Map<String, String> nameValuePairMap) {
        final String value = nameValuePairMap.get(propertyName);
        return value == null ? null : value.trim();
    }

    public static boolean haveEqualValues(final CertificateEntity first, final CertificateEntity second) {
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

        if (first.getCertificateIdentification() == null) {
            if (second.getCertificateIdentification() != null) {
                return false;
            }
        } else if (!first.getCertificateIdentification().equals(second.getCertificateIdentification())) {
            return false;
        }

        if (first.getLoadHistoryEntity() == null) {
            if (second.getLoadHistoryEntity() != null) {
                return false;
            }
        } else if (!LoadHistoryEntity.haveEqualValues(first.getLoadHistoryEntity(),
                second.getLoadHistoryEntity())) {
            return false;
        }

        if (first.getUidSource() == null) {
            if (second.getUidSource() != null) {
                return false;
            }
        } else if (!first.getUidSource().equals(second.getUidSource())) {
            return false;
        }

        if (first.getCertificateDocument() == null) {
            if (second.getCertificateDocument() != null) {
                return false;
            }
        } else if (!first.getCertificateDocument().equals(second.getCertificateDocument())) {
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

        if (first.getFractionUnit() == null) {
            if (second.getFractionUnit() != null) {
                return false;
            }
        } else if (!first.getFractionUnit().equals(second.getFractionUnit())) {
            return false;
        }

        if (first.getUncertainty() == null) {
            if (second.getUncertainty() != null) {
                return false;
            }
        } else if (!first.getUncertainty().equals(second.getUncertainty())) {
            return false;
        }

        if (first.getUncertaintyNumberOfSigma() == null) {
            if (second.getUncertaintyNumberOfSigma() != null) {
                return false;
            }
        } else if (!first.getUncertaintyNumberOfSigma().equals(second.getUncertaintyNumberOfSigma())) {
            return false;
        }
        return true;
    }

}
