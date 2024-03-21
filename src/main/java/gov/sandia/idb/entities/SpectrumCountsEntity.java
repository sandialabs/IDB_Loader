package gov.sandia.idb.entities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SpectrumCountsEntity {

    private Integer spectrumCountsId;
    private SpectrumAcquisitionEntity spectrumAcquisitionEntity;
    private Integer uidSpectrum;
    private String uidCounts;

    public Integer getSpectrumCountsId() {
        return spectrumCountsId;
    }

    public void setSpectrumCountsId(Integer spectrumCountsId) {
        this.spectrumCountsId = spectrumCountsId;
    }

    public Integer getUidSpectrum() {
        return uidSpectrum;
    }

    public void setUidSpectrum(final Integer uidSpectrum) {
        this.uidSpectrum = uidSpectrum;
    }

    public String getUidCounts() {
        return uidCounts;
    }

    public void setUidCounts(String uidCounts) {
        this.uidCounts = uidCounts;
    }

    public SpectrumAcquisitionEntity getSpectrumAcquisitionEntity() {
        return spectrumAcquisitionEntity;
    }

    public void setSpectrumAcquisitionEntity(SpectrumAcquisitionEntity spectrumAcquisitionEntity) {
        this.spectrumAcquisitionEntity = spectrumAcquisitionEntity;
    }

    @Override
    public String toString() {
        return "SpectrumCountsEntity [spectrumCountsId=" + spectrumCountsId + ", spectrumAcquistionEntity="
                + spectrumAcquisitionEntity + ", uidSpectrum=" + uidSpectrum + ", uidCounts=" + uidCounts + "]";
    }

    private static final String SPECCTRUM_DATA_COUNTS = "configuration.acquisition.counts";
    final static private String UID_SPECTRUM_NAME_VALUE_PAIR = "UID.spectrum";

    final private static Map<Integer, SpectrumCountsEntity> cache = new HashMap<>();
    private static final String INSERT_SPECTRUM_COUNTS = " INSERT INTO spectrum_counts_table ( "
            + "   spectrum_acquisition_id, uid_spectrum, spectrum_data_counts ) "
            + " VALUES ( ?,?,? ); ";

    private static final String SELECTSPECTRUM_COUNTS = " SELECT spectrum_counts_id "
            + " FROM spectrum_counts_table AS sc "
            + "  WHERE spectrum_acquisition_id = (?) "
            + "   AND  uid_spectrum = (?) "
            + "   AND  spectrum_data_counts = (?); ";

    public static SpectrumCountsEntity load(final Connection conn,
            final Map<String, String> nameValuePairs) {

        SpectrumCountsEntity entity = SpectrumCountsEntity.find(conn, nameValuePairs);

        // Not enough information to make an entity...
        if (entity == null) {
            throw new RuntimeException("Not enough information to make spectrum_counts: " + nameValuePairs);
        }

        // found entity in cache, so return it...
        if (entity.getSpectrumCountsId() != null) {
            return entity;
        }

        // need to create a new entity. Don't need to
        // check for NULL when setting paramter index
        // because all fields should always have a 
        // non-null value. 
        try (final PreparedStatement statement = conn.prepareStatement(
                SpectrumCountsEntity.INSERT_SPECTRUM_COUNTS,
                Statement.RETURN_GENERATED_KEYS)) {

            statement.setInt(1, entity.getSpectrumAcquisitionEntity().getSpectrumAcquistionId());
            statement.setInt(2, entity.getUidSpectrum());
            statement.setString(3, entity.getUidCounts());

            statement.executeUpdate();
            try (final ResultSet rs = statement.getGeneratedKeys()) {
                if (rs.next()) {
                    final Integer key = Integer.valueOf(rs.getInt(1));
                    if (rs.wasNull()) {
                        throw new RuntimeException("Key is null.");
                    }
                    entity.setSpectrumCountsId(key);
                    SpectrumCountsEntity.cache.put(key, entity);
                    return entity;
                }
            }

        } catch (final Exception e) {
            throw new RuntimeException("Inserting spectrum count values from pairs: " + nameValuePairs, e);
        }
        return null;
    }

    private static final SpectrumCountsEntity find(final Connection conn,
            final Map<String, String> nameValuePairs) {

        final String uidSpectrumString = nameValuePairs.get(SpectrumCountsEntity.UID_SPECTRUM_NAME_VALUE_PAIR);
        final String spectrumDataCounts = nameValuePairs.get(SpectrumCountsEntity.SPECCTRUM_DATA_COUNTS);

        final Integer uidSpectrum = Integer.valueOf(uidSpectrumString);

        final SpectrumCountsEntity entity = new SpectrumCountsEntity();
        entity.setSpectrumAcquisitionEntity(SpectrumAcquisitionEntity.find(uidSpectrum));
        entity.setUidSpectrum(uidSpectrum);
        entity.setUidCounts(spectrumDataCounts);

        for (final SpectrumCountsEntity cachedEntity : SpectrumCountsEntity.cache.values()) {
            if (SpectrumCountsEntity.haveEqualValues(entity, cachedEntity)) {
                return cachedEntity;
            }
        }

        try (final PreparedStatement statement = conn.prepareStatement(
            SpectrumCountsEntity.SELECTSPECTRUM_COUNTS)) {

        statement.setInt(1, entity.getSpectrumAcquisitionEntity().getSpectrumAcquistionId());
        statement.setInt(2, entity.getUidSpectrum());
        statement.setString(3, entity.getUidCounts());

        
        try (final ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                entity.setSpectrumCountsId(rs.getInt("spectrum_counts_id"));
                SpectrumCountsEntity.cache.put(entity.getSpectrumCountsId(), entity);
                return entity;
            }
        }

    } catch (final Exception e) {
        throw new RuntimeException("Retrieving entity from database: " + nameValuePairs, e);
    }
        // this spectrum counts entity has new values that need to be added to the
        // measurement table, so return the non-cached entity...
        return entity;
    }

    public static boolean haveEqualValues(final SpectrumCountsEntity first,
            final SpectrumCountsEntity second) {
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

        if (first.getUidCounts() == null) {
            if (second.getUidCounts() != null) {
                return false;
            }
        } else if (!first.getUidCounts().equals(second.getUidCounts())) {
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
