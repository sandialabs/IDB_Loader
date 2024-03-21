USE IDB;


/* ****************************************************
 **   Maria DB TABLE CREATION FILE
 **
**************************************************/


/***************************************************************
 **  Analyzer Table
 **************************************************************/
CREATE TABLE analyzer_table (
    analyzer_id INT AUTO_INCREMENT NOT NULL,
    analyzer_name VARCHAR(200) NULL,
    CONSTRAINT pk_analyzer_table PRIMARY KEY (analyzer_id),
    CONSTRAINT uq_analyzer_table_name UNIQUE (analyzer_name) 
);

/**************************************
 * Attenuator Table
 * *********************************** */
CREATE TABLE attenuator_table (
    attenuator_id INT AUTO_INCREMENT NOT NULL,
    material VARCHAR(100) NULL,
    thickness_mm FLOAT NULL,
    approximation TINYINT(1) NULL,
    minimum_mm FLOAT NULL,
    maximum_mm FLOAT NULL,
    CONSTRAINT  pk_attenuator_table PRIMARY KEY (attenuator_id)
);


/**************************************
 * Attenuator Measurement Join Table
 * *********************************** */
CREATE TABLE attenuator_measurement_join_table (
    attenuator_configuration_join_id INT AUTO_INCREMENT NOT NULL,
    attenuator_id INT NOT NULL,
    measurement_id INT NOT NULL,
    CONSTRAINT pk_attenuator_configuration_join_table PRIMARY KEY (attenuator_configuration_join_id)
);

/***************************************************************
 **  CertificateTable
 **************************************************************/
CREATE TABLE certificate_table (
    certificate_id INT AUTO_INCREMENT NOT NULL,
    load_history_id INT NOT NULL,
    source_type_id INT NOT NULL,
    uid_source INT NOT NULL,
    certificate_identification VARCHAR(209) NULL,
    certificate_document VARCHAR(300) NULL,
    fraction_unit VARCHAR(100) NULL,
    uncertainty VARCHAR(100) NULL,
    uncertainty_number_of_sigma VARCHAR(100),
    CONSTRAINT pk_certificate_table PRIMARY KEY (certificate_id),
    CONSTRAINT uq_certifcate_table_load_history_uid_source UNIQUE (load_History_id, uid_source) 
);

/***************************************************************
 **  CertificateMOXTable
 **************************************************************/
CREATE TABLE certificate_mox_table (
    certificate_mox_id INT AUTO_INCREMENT NOT NULL,
    certificate_id INT NOT NULL,
    reference_date DATETIME NULL,
    pu_238_value FLOAT NULL,
    pu_238_sigma FLOAT NULL,
    pu_239_value FLOAT NULL,
    pu_239_sigma FLOAT NULL,
    pu_240_value FLOAT NULL,
    pu_240_sigma FLOAT NULL,
    pu_241_value FLOAT NULL,
    pu_241_sigma FLOAT NULL,
    pu_242_value FLOAT NULL,
    pu_242_sigma FLOAT NULL,
    am_241_value FLOAT NULL,
    am_241_sigma FLOAT NULL,
    mox_u_percent FLOAT NULL,
    mox_pu_percent FLOAT NULL,
    mox_u_pu_ratio_value FLOAT NULL,
    mox_u_pu_ratio_sigma FLOAT NULL,
    pu_mass_value FLOAT NULL,
    pu_mass_sigma FLOAT NULL,
    pu_content_value FLOAT NULL,
    pu_content_sigma FLOAT NULL,
    mox_u_234_value FLOAT NULL,
    mox_u_234_sigma FLOAT NULL,
    mox_u_235_value FLOAT NULL,
    mox_u_235_sigma FLOAT NULL,
    mox_u_236_value FLOAT NULL,
    mox_u_236_sigma FLOAT NULL,
    mox_u_238_value FLOAT NULL,
    mox_u_238_sigma FLOAT NULL,
    pu_240eff_value FLOAT NULL,
    pu_240eff_sigma FLOAT NULL,
    separation_date DATETIME NULL,
    CONSTRAINT pk_certificate_mox_table PRIMARY KEY (certificate_mox_id)
);

/***************************************************************
** Certificate Note Table. 
****************************************************************/
CREATE TABLE certificate_note_table (
    certificate_note_id INT AUTO_INCREMENT NOT NULL,
    certificate_id INT NOT NULL,
    certificate_note VARCHAR(500) NOT NULL,
    CONSTRAINT  pk_certificate_note_table PRIMARY KEY (certificate_note_id)
);

/***************************************************************
 **  CertificatePlutoniumTable
 **************************************************************/
CREATE TABLE certificate_pu_table (
    certificate_pu_id INT AUTO_INCREMENT NOT NULL,
    certificate_id INT NOT NULL,
    reference_date DATETIME NULL,
    pu_238_value FLOAT NULL,
    pu_238_sigma FLOAT NULL,
    pu_239_value FLOAT NULL,
    pu_239_sigma FLOAT NULL,
    pu_240_value FLOAT NULL,
    pu_240_sigma FLOAT NULL,
    pu_241_value FLOAT NULL,
    pu_241_sigma FLOAT NULL,
    pu_242_value FLOAT NULL,
    pu_242_sigma FLOAT NULL,
    am_241_value FLOAT NULL,
    am_241_sigma FLOAT NULL,
    supplementary_pu_mass_value FLOAT NULL,
    supplementary_pu_mass_sigma FLOAT NULL,
    supplementary_pu_content_value FLOAT NULL,
    supplementary_pu_content_sigma FLOAT NULL,
    pu_240eff_value FLOAT NULL,
    pu_240eff_sigma FLOAT NULL,
    separation_date DATETIME NULL,
    CONSTRAINT pk_certificate_pu_table PRIMARY KEY (certificate_pu_id)
);

/***************************************************************
 **  CertificateUraniumTable
 **************************************************************/
CREATE TABLE certificate_u_table (
    certificate_u_id INT AUTO_INCREMENT NOT NULL,
    certificate_id INT NOT NULL,
    reference_date DATETIME NULL,
    separation_date DATETIME NULL,
    u_234_value FLOAT NULL,
    u_234_sigma FLOAT NULL,
    u_235_value FLOAT NULL,
    u_235_sigma FLOAT NULL,
    u_236_value FLOAT NULL,
    u_236_sigma FLOAT NULL,
    u_238_value FLOAT NULL,
    u_238_sigma FLOAT NULL,       
    CONSTRAINT pk_certificate_u_tabe PRIMARY KEY (certificate_u_id)
);

/***************************************************************
 **  Configuration Table
 **************************************************************/
CREATE TABLE configuration_table (
    configuration_id INT AUTO_INCREMENT NOT NULL,
    detector_id INT NULL,
    distance FLOAT NULL,
    CONSTRAINT pk_configuration_table PRIMARY KEY (configuration_id),
    CONSTRAINT uq_configuration_table_detector_distance UNIQUE (detector_id, distance) 
);


/***************************************************************
 **  Detector Table
 **************************************************************/
CREATE TABLE detector_table (
    detector_id INT AUTO_INCREMENT NOT NULL,
    detector_type_id INT NULL,
    analyzer_id INT NULL,
    CONSTRAINT pk_detector_table PRIMARY KEY (detector_id),
    CONSTRAINT uq_detector_table_detector_type_analyzer UNIQUE (detector_type_id, analyzer_id) 
);

/***************************************************************
 **  Detector Settings Table
 **************************************************************/
CREATE TABLE detector_settings_table (
    detector_settings_id INT AUTO_INCREMENT NOT NULL,
    detector_id INT NOT NULL,
    detector_fwhm FLOAT NULL, 
    analyzer_gain FLOAT NULL,
    analyzer_offset FLOAT NULL,
    energy_range VARCHAR(100) NULL,
    number_of_channels INT NULL,
    CONSTRAINT  pk_detector_settings_table PRIMARY KEY (detector_settings_id)
);

/***************************************************************
 **  DetectorType Table
 **************************************************************/
CREATE TABLE detector_type_table (
    detector_type_id INT AUTO_INCREMENT NOT NULL,
    detector_material_id INT NULL,
    detector_geometry_id INT NULL,
    CONSTRAINT pk_detector_type_table PRIMARY KEY (detector_type_id),
    CONSTRAINT uq_detector_type_table_material_geometry UNIQUE (detector_material_id, detector_geometry_id) 
);

/***************************************************************
 **  DetectorMaterial Table
 **************************************************************/
CREATE TABLE detector_material_table (
    detector_material_id INT AUTO_INCREMENT NOT NULL,
    detector_material_name VARCHAR(20) NULL,
    CONSTRAINT pk_detector_material_table PRIMARY KEY (detector_material_id),
    CONSTRAINT uq_detector_material_table_detector_material_name UNIQUE (detector_material_name) 
);

/***************************************************************
 **  DetectorGeometry Table
 **************************************************************/
CREATE TABLE detector_geometry_table (
    detector_geometry_id INT AUTO_INCREMENT NOT NULL,
    detector_geometry_name VARCHAR(20) NULL,
    CONSTRAINT pk_detector_geometry_table PRIMARY KEY (detector_geometry_id),
    CONSTRAINT uq_detector_material_geometry_detector_geometry UNIQUE (detector_geometry_name) 
);

/***************************************************************
** Element Table 
****************************************************************/
CREATE TABLE element_table (
    element_id INT AUTO_INCREMENT NOT NULL,
    element_name VARCHAR(20) NOT NULL,
    element_symbol VARCHAR(2) NOT NULL,
    CONSTRAINT  uq_element_table_element_name UNIQUE (element_name),
    CONSTRAINT  uq_element_table_element_symbol UNIQUE (element_symbol),
    CONSTRAINT  pk_element_table PRIMARY KEY (element_id)
);

/***************************************************************
** Isotope Table 
****************************************************************/
CREATE TABLE isotope_table (
    isotope_id INT AUTO_INCREMENT NOT NULL,
    element_id INT NOT NULL,
    isotope_number INT NOT NULL,
    isotope_mass_activity FLOAT NULL,
    CONSTRAINT  uq_isotope_table_element_id_isotope_number UNIQUE (element_id, isotope_number),
    CONSTRAINT  pk_isotope_table PRIMARY KEY (isotope_id)
);

/***************************************************************
** Load History Table 
****************************************************************/
CREATE TABLE load_history_table (
    load_history_id INT AUTO_INCREMENT NOT NULL,
    data_provider VARCHAR(300) NULL,
    laboratory VARCHAR(300) NULL,
    contact_email VARCHAR(300) NULL,
    load_history_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_history_description VARCHAR(300) NOT NULL,
    CONSTRAINT  uq_load_history_table_load_history_description UNIQUE (load_history_description),
    CONSTRAINT  pk_load_history_table PRIMARY KEY (load_history_id)
);

/***************************************************************
** Measurement Table 
****************************************************************/
CREATE TABLE measurement_table (
    measurement_id INT AUTO_INCREMENT NOT NULL,
    uid_metadata INT NULL,
    uid_source INT NULL,
    certificate_id INT NULL,
    sample_id INT NOT NULL,
    configuration_id INT NULL,
    supplementary_measurement_id INT NULL,
    CONSTRAINT  pk_measurement_table PRIMARY KEY (measurement_id),
    CONSTRAINT  uq_measurement_table_certificate_sample_configuration
        UNIQUE (certificate_id, sample_id, configuration_id)
);

/***************************************************************
** Measurement Note Table. 
****************************************************************/
CREATE TABLE measurement_note_table (
    measurement_note_id INT AUTO_INCREMENT NOT NULL,
    measurement_id INT NOT NULL,
    measurement_note VARCHAR(500) NOT NULL,
    CONSTRAINT  pk_measurement_note_table PRIMARY KEY (measurement_note_id)
);


/********************************************************** 
*  Sample Table
* ******************************************************** */
CREATE TABLE sample_table (
    sample_id INT AUTO_INCREMENT NOT NULL,
    sample_chemical_composition_id INT NULL,
    source_type_id INT NULL,
    sample_material_form_id INT NULL,
    sample_mox_u_pu_ratio_value FLOAT NULL,
    CONSTRAINT  pk_sample_table PRIMARY KEY (sample_id)  
);

/**********************************************************
* Sample Chemical Composition
*************************************************************** */
CREATE TABLE sample_chemical_composition_table (
    sample_chemical_composition_id INT AUTO_INCREMENT NOT NULL,
    sample_chemical_composition_name VARCHAR(100) NULL,
    CONSTRAINT  pk_sample_chemical_composition_table PRIMARY KEY (sample_chemical_composition_id),
    CONSTRAINT uq_sample_chemical_composition_table_sample_name UNIQUE (sample_chemical_composition_name)     
);


/***************************************************************
** Sample Mass Fraction Table 
****************************************************************/
CREATE TABLE sample_mass_fraction_table (
    mass_fraction_id INT AUTO_INCREMENT NOT NULL,
    sample_id INT NOT NULL,
    isotope_id INT NOT NULL,
    isotope_mass_fraction FLOAT NULL,
    isotope_mass_fraction_notes VARCHAR(200) NULL,
    CONSTRAINT  pk_mass_fraction_table PRIMARY KEY (mass_fraction_id),
    CONSTRAINT  uq_mass_fraction_table_sample_isotope UNIQUE (sample_id, isotope_id)
);

/**********************************************************
* Sample Material Form
**************************************************************/
CREATE TABLE sample_material_form_table (
    sample_material_form_id INT AUTO_INCREMENT NOT NULL,
    sample_material_form_name VARCHAR(20) NULL,
    CONSTRAINT  pk_sample_material_form_table PRIMARY KEY (sample_material_form_id),
    CONSTRAINT  uq_sample_material_form_table_sample_material_form_Name UNIQUE (sample_material_form_name)
);


/***************************************************************
** Source Type Table -- an enumeration of the source
**  material type. Possible values are:
**   MOX
**   PU
**   U
***************************************************************/
CREATE TABLE source_type_table (
    source_type_id INT AUTO_INCREMENT NOT NULL,
    source_type_name VARCHAR(10) NOT NULL,
    source_type_description VARCHAR(200) NULL,
    CONSTRAINT  pk_source_type_table PRIMARY KEY (source_type_id),
    CONSTRAINT  uq_source_type_table_source_type_name UNIQUE (source_type_name)
);

/***************************************************************
** Spectrum Acquisition Table
******************************************************************/
CREATE TABLE spectrum_acquisition_table  (
    spectrum_acquisition_id INT AUTO_INCREMENT NOT NULL,
    spectrum_measurement_id INT NOT NULL,
    uid_spectrum INT NOT NULL,
    acquisition_date_time DATETIME NULL,
    total_counts INT NULL,
    real_time_seconds FLOAT NULL,
    live_time_seconds FLOAT NULL,
    dead_time FLOAT NULL,    
    count_rate FLOAT NULL,
    CONSTRAINT  pk_spectrum_acquisition_table PRIMARY KEY (spectrum_acquisition_id)
);

/***************************************************************
** Spectrum Acquisition Note Table. 
****************************************************************/
CREATE TABLE spectrum_acquisition_note_table (
    spectrum_acquisition_note_id INT AUTO_INCREMENT NOT NULL,
    spectrum_acquisition_id INT NOT NULL,
    spectrum_acquisition_note VARCHAR(500) NOT NULL,
    CONSTRAINT  pk_spectrum_acquisition_note_table PRIMARY KEY (spectrum_acquisition_note_id)
);

/***************************************************************
** Spectrum Checksum Table
******************************************************************/
CREATE TABLE spectrum_checksum_table  (
    spectrum_checksum_id INT AUTO_INCREMENT NOT NULL,
    spectrum_acquisition_id INT NOT NULL,
    uid_spectrum INT NOT NULL,
    spectrum_md5_checksum CHAR(32) NOT NULL,
    CONSTRAINT  PK_spectrum_checksum_table PRIMARY KEY (spectrum_checksum_id),
    CONSTRAINT  uq_spectrum_checksum_table_md5_checksum UNIQUE (spectrum_md5_checksum)
);

/***************************************************************
** Spectrum Counts Table
******************************************************************/
CREATE TABLE spectrum_counts_table  (
    spectrum_counts_id INT AUTO_INCREMENT NOT NULL,
    spectrum_acquisition_id INT NOT NULL,
    uid_spectrum INT NOT NULL,
    spectrum_data_counts JSON NOT NULL,
    CONSTRAINT  PK_spectrum_counts_table PRIMARY KEY (spectrum_counts_id)
);

/***************************************************************
** Table: Spectrum Measurement Table
******************************************************************/
CREATE TABLE spectrum_measurement_table  (
    spectrum_measurement_id INT AUTO_INCREMENT NOT NULL,
    measurement_id INT NULL,
    detector_settings_id INT NULL,
    CONSTRAINT pk_spectrum_measurement_table PRIMARY KEY (spectrum_measurement_id)
);

/***************************************************************
** Supplementary Measurement Table
***************************************************************/
CREATE TABLE supplementary_measurement_table (
    supplementary_measurement_id INT AUTO_INCREMENT NOT NULL,
    measurement_detector_size VARCHAR(100) NULL,
    measurement_electronics VARCHAR(200) NULL,
    measurement_geometry VARCHAR(500) NULL,
    CONSTRAINT  pk_supplementary_measurement_id PRIMARY KEY (supplementary_measurement_id)
);

