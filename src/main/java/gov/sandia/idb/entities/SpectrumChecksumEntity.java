package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SpectrumChecksumEntity {

    private Integer spectrumChecksumId;
    private SpectrumAcquisitionEntity spectrumAcquisitionEntity;
    private Integer uidSpectrum;
    private String md5Checksum;

    public Integer getSpectrumChecksumId() {
        return spectrumChecksumId;
    }

    public void setSpectrumChecksumId(Integer spectrumChecksumId) {
        this.spectrumChecksumId = spectrumChecksumId;
    }

    public Integer getUidSpectrum() {
        return uidSpectrum;
    }

    public void setUidSpectrum(Integer uidSpectrum) {
        this.uidSpectrum = uidSpectrum;
    }

    public String getMd5Checksum() {
        return md5Checksum;
    }

    public void setMd5Checksum(String md5Checksum) {
        this.md5Checksum = md5Checksum;
    }

    public SpectrumAcquisitionEntity getSpectrumAcquisitionEntity() {
        return spectrumAcquisitionEntity;
    }

    public void setSpectrumAcquisitionEntity(SpectrumAcquisitionEntity spectrumAcquisitionEntity) {
        this.spectrumAcquisitionEntity = spectrumAcquisitionEntity;
    }

    @Override
    public String toString() {
        return "SpectrumChecksumEntity [spectrumChecksumId=" + spectrumChecksumId + ", spectrumAcquisitionEntity="
                + spectrumAcquisitionEntity + ", uidSpectrum=" + uidSpectrum + ", md5Checksum="
                + md5Checksum + "]";
    }

    final static private String UID_SPECTRUM_NAME_VALUE_PAIR = "UID.spectrum";
    final static private String SPECTRA_MD5 = "spectra.md5";

    final private static Map<Integer, SpectrumChecksumEntity> cache = new HashMap<>();
    private static final String INSERT_SPECTRUM_CHECKSUM = " INSERT INTO spectrum_checksum_table ( "
            + "   spectrum_acquisition_id, uid_spectrum, spectrum_md5_checksum ) "
            + " VALUES ( ?,?,? ); ";
    private static final String SELECT_SPECTRUM_CHECKSUM = " SELECT spectrum_checksum_id "
            + " FROM spectrum_checksum_table AS sc "
            + " WHERE spectrum_acquisition_id = (?) "
            + "  AND  uid_spectrum = (?) "
            + "  AND  spectrum_md5_checksum = (?); ";

    public static SpectrumChecksumEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SpectrumChecksumEntity entity = SpectrumChecksumEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make spectrum_checksum: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getSpectrumChecksumId() != null) {
            return entity;
        }

        // need to create a new entity. Don't need to
        // worry about null values becasue all entries
        // should always have non-null value.
        try (final PreparedStatement statement = conn.prepareStatement(
                SpectrumChecksumEntity.INSERT_SPECTRUM_CHECKSUM,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, SpectrumAcquisitionEntity.find(entity.getUidSpectrum()).getSpectrumAcquistionId());
            statement.setInt(2, entity.getUidSpectrum());
            statement.setString(3, entity.getMd5Checksum());

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSpectrumChecksumId(key);
                    SpectrumChecksumEntity.cache.put(key, entity);

                }
            }
            return entity;
        } catch (final Exception e) {
            throw new RuntimeException("Inserting checksum values from pairs: " + nameValuePairs, e);
        }

    }

    private static final SpectrumChecksumEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String uidSpectrumString = nameValuePairs.get(SpectrumChecksumEntity.UID_SPECTRUM_NAME_VALUE_PAIR);
        final String spectraMd5 = nameValuePairs.get(SpectrumChecksumEntity.SPECTRA_MD5);

        final Integer uidSpectrum = Integer.valueOf(uidSpectrumString);

        final SpectrumChecksumEntity entity = new SpectrumChecksumEntity();
        entity.setSpectrumAcquisitionEntity(SpectrumAcquisitionEntity.find(uidSpectrum));
        entity.setUidSpectrum(uidSpectrum);
        entity.setMd5Checksum(spectraMd5);

        for (final SpectrumChecksumEntity cachedEntity : SpectrumChecksumEntity.cache.values()) {
            if (SpectrumChecksumEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        // not in cache, check database..
        try (final PreparedStatement statement = conn.prepareStatement(
                SpectrumChecksumEntity.SELECT_SPECTRUM_CHECKSUM)) {

            statement.setInt(1, SpectrumAcquisitionEntity.find(entity.getUidSpectrum()).getSpectrumAcquistionId());
            statement.setInt(2, entity.getUidSpectrum());
            statement.setString(3, entity.getMd5Checksum());

            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    entity.setSpectrumChecksumId(Integer.valueOf(rs.getInt("spectrum_checksum_id")));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    SpectrumChecksumEntity.cache.put(entity.getSpectrumChecksumId(), entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting checksum values from pairs: " + nameValuePairs, e);
        }

        // not in cache or database...
        return entity;
    }

    public static boolean haveEqualValues(final SpectrumChecksumEntity first,
            final SpectrumChecksumEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getUidSpectrum() == null) {
            if (second.getUidSpectrum() != null) {
                return false;
            }
        } else if (!first.getUidSpectrum().equals(second.getUidSpectrum())) {
            return false;
        }

        if (first.getMd5Checksum() == null) {
            if (second.getMd5Checksum() != null) {
                return false;
            }
        } else if (!first.getMd5Checksum().equals(second.getMd5Checksum())) {
            return false;
        }

        if (first.getSpectrumAcquisitionEntity() == null) {
            if (second.getSpectrumAcquisitionEntity() != null) {
                return false;
            }
        } else if (!SpectrumAcquisitionEntity.haveEqualValues(first.getSpectrumAcquisitionEntity(),
                second.getSpectrumAcquisitionEntity())) {
            return false;
        }
        return true;
    }
}
