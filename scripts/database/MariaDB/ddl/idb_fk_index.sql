USE IDB;


/* ****************************************************
 **   MariaDB Foreign Key Creation Table
 **
**************************************************/


/**************************************
 * Attenuator Configuration Join Table
 * *********************************** */
 ALTER TABLE attenuator_measurement_join_table 
   ADD CONSTRAINT fk_attenuator_measurement_join_attenuator
     FOREIGN KEY (attenuator_id)
     REFERENCES attenuator_table(attenuator_id);  
 ALTER TABLE attenuator_measurement_join_table 
   ADD CONSTRAINT fk_attenuator_measurement_join_configuration
     FOREIGN KEY (measurement_id)
     REFERENCES measurement_table(measurement_id);      

/***************************************************************
 **  CertificateTable
 **************************************************************/
 ALTER TABLE certificate_table 
   ADD CONSTRAINT fk_certificate_load_history
     FOREIGN KEY (load_history_id)
     REFERENCES load_history_table(load_history_id);  
 ALTER TABLE certificate_table 
   ADD CONSTRAINT fk_certificate_source_type
     FOREIGN KEY (source_type_id)
     REFERENCES source_type_table(source_type_id);   

/***************************************************************
 **  CertificateMOXTable
 **************************************************************/
 ALTER TABLE certificate_mox_table 
   ADD CONSTRAINT fk_certificate_mox_certificate
     FOREIGN KEY (certificate_id)
     REFERENCES certificate_table(certificate_id); 
     
/***************************************************************
 **  certificate_note_table
 **************************************************************/
 ALTER TABLE certificate_note_table 
   ADD CONSTRAINT fk_certificate_note_certificate
     FOREIGN KEY (certificate_id)
     REFERENCES certificate_table(certificate_id); 

/***************************************************************
 **  CertificatePlutoniumTable
 **************************************************************/
 ALTER TABLE certificate_pu_table 
   ADD CONSTRAINT fk_certificate_pu_certificate
     FOREIGN KEY (certificate_id)
     REFERENCES certificate_table(certificate_id); 

/***************************************************************
 **  CertificateUraniumTable
 **************************************************************/
 ALTER TABLE certificate_u_table 
   ADD CONSTRAINT fk_certificate_u_certificate
     FOREIGN KEY (certificate_id)
     REFERENCES certificate_table(certificate_id); 

/***************************************************************
 **  Configuration Table
 **************************************************************/
ALTER TABLE configuration_table
   ADD CONSTRAINT fk_configuration_table_detector
     FOREIGN KEY (detector_id)
     REFERENCES detector_table(detector_id);


/***************************************************************
 **  Detector Table
 **************************************************************/
ALTER TABLE detector_table 
   ADD CONSTRAINT fk_detector_table_detector_type
     FOREIGN KEY (detector_type_id)
     REFERENCES detector_type_table(detector_type_id);
 ALTER TABLE detector_table 
   ADD CONSTRAINT fk_detector_table_analyzer
     FOREIGN KEY (analyzer_id)
     REFERENCES analyzer_table(analyzer_id);

     
/***************************************************************
 **  Detector Settings Table
 **************************************************************/
ALTER TABLE detector_settings_table 
   ADD CONSTRAINT fk_detector_settings_table_detector
     FOREIGN KEY (detector_id)
     REFERENCES detector_table(detector_id);

/***************************************************************
 **  DetectorType Table
 **************************************************************/
ALTER TABLE detector_type_table 
   ADD CONSTRAINT fk_detector_type_table_detector_material
     FOREIGN KEY (detector_material_id)
     REFERENCES detector_material_table(detector_material_id);
 ALTER TABLE detector_type_table 
   ADD CONSTRAINT fk_detector_type_table_detector_geometry
     FOREIGN KEY (detector_geometry_id)
     REFERENCES detector_geometry_table(detector_geometry_id);

/***************************************************************
** Isotope Table 
****************************************************************/
 ALTER TABLE isotope_table 
   ADD CONSTRAINT fk_isotope_table_element
     FOREIGN KEY (element_id)
     REFERENCES element_table(element_id);

/***************************************************************
** Measurement Table 
****************************************************************/
 ALTER TABLE measurement_table 
   ADD CONSTRAINT fk_measurement_table_certificate
     FOREIGN KEY (certificate_id)
     REFERENCES certificate_table(certificate_id);
 ALTER TABLE measurement_table 
   ADD CONSTRAINT fk_measurement_table_source
     FOREIGN KEY (sample_id)
     REFERENCES sample_table(sample_id);
ALTER TABLE measurement_table 
   ADD CONSTRAINT fk_measurement_table_configuration
     FOREIGN KEY (configuration_id)
     REFERENCES configuration_table(configuration_id);
 ALTER TABLE measurement_table 
   ADD CONSTRAINT fk_measurement_table_supplementary_measurement
     FOREIGN KEY (supplementary_measurement_id)
     REFERENCES supplementary_measurement_table(supplementary_measurement_id);


/***************************************************************
** Measurement Note Table
******************************************************************/
 ALTER TABLE measurement_note_table 
   ADD CONSTRAINT fk_measurement_note_table_measurement
     FOREIGN KEY (measurement_id)
     REFERENCES measurement_table(measurement_id);
  
     
/***********************************************************
*  Sample Table
* ******************************************************** */
 ALTER TABLE sample_table 
   ADD CONSTRAINT fk_sample_table_sample_chemical_composition
     FOREIGN KEY (sample_chemical_composition_id)
     REFERENCES sample_chemical_composition_table(sample_chemical_composition_id);
 ALTER TABLE sample_table 
   ADD CONSTRAINT fk_sample_table_source_type
     FOREIGN KEY (source_type_id)
     REFERENCES source_type_table(source_type_id);
 ALTER TABLE sample_table 
   ADD CONSTRAINT fk_sample_table_sample_material_form
     FOREIGN KEY (sample_material_form_id)
     REFERENCES sample_material_form_table(sample_material_form_id);


/***************************************************************
** Sample Mass Fraction Table 
****************************************************************/
 ALTER TABLE sample_mass_fraction_table 
   ADD CONSTRAINT fk_sample_mass_fraction_table_source
     FOREIGN KEY (sample_id)
     REFERENCES sample_table(sample_id);
 ALTER TABLE sample_mass_fraction_table 
   ADD CONSTRAINT fk_sample_mass_fraction_table_isotope
     FOREIGN KEY (isotope_id)
     REFERENCES isotope_table(isotope_id);


/***************************************************************
** Spectrum Acquisition Table
******************************************************************/
 ALTER TABLE spectrum_acquisition_table 
   ADD CONSTRAINT fk_spectrum_acquisition_table_spectrum_measurement
     FOREIGN KEY (spectrum_measurement_id)
     REFERENCES spectrum_measurement_table(spectrum_measurement_id);

/***************************************************************
** Spectrum Acquisition Note Table
******************************************************************/
 ALTER TABLE spectrum_acquisition_note_table 
   ADD CONSTRAINT fk_spectrum_acquisition_note_table_spectrum_acquistion
     FOREIGN KEY (spectrum_acquisition_id)
     REFERENCES spectrum_acquisition_table(spectrum_acquisition_id);

 
/***************************************************************
** Spectrum Checksum Table
******************************************************************/
 ALTER TABLE spectrum_checksum_table 
   ADD CONSTRAINT fk_spectrum_checksum_table_spectrum_acquistion
     FOREIGN KEY (spectrum_acquisition_id)
     REFERENCES spectrum_acquisition_table(spectrum_acquisition_id);

/***************************************************************
** Spectrum Counts Table
******************************************************************/
 ALTER TABLE spectrum_counts_table 
   ADD CONSTRAINT fk_spectrum_counts_table_spectrum_acquistion
     FOREIGN KEY (spectrum_acquisition_id)
     REFERENCES spectrum_acquisition_table(spectrum_acquisition_id);

/***************************************************************
** Table: Spectrum Measurement Table
******************************************************************/
 ALTER TABLE spectrum_measurement_table 
   ADD CONSTRAINT fk_spectrum_measurement_table_measurement
     FOREIGN KEY (measurement_id)
     REFERENCES measurement_table(measurement_id);
 ALTER TABLE spectrum_measurement_table 
   ADD CONSTRAINT fk_spectrum_measurement_table_detector_settings
     FOREIGN KEY (detector_settings_id)
     REFERENCES detector_settings_table(detector_settings_id);


