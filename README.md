# IDB Data Loader
The International Database of Reference Gamma-Ray Spectra of Various Nuclear Matter (IDB) is designed to hold curated gamma spectral data that has been formatted as described in [**Preparation of the IDB Spectra LLNL-TR-829556.pdf**](https://github.com/sandialabs/watchr-core/tree/main/src/main/resources/docs). The tables and relationships that make up the IDB are described in [**IDB Database Tables 2401043.pdf**](https://github.com/sandialabs/IDB-Data-Loader/tree/main/docs). The IDB Data Loader program uploads one set of sprectral data that is in the CSV format described in "Preparation of the IDB Spectra" into an existing instance of an IDB database. The data loader is described more fully in [**IDB_Data_Loader_v3.docx.pdf**](https://github.com/sandialabs/IDB-Data-Loader/tree/main/docs).


## Setup

The IDB Data Loader is a Java desktop application that connects to a MariaDB database using Java Data Base Connectivity (JDBC). The machine on which the Data Loader application is going to run needs:

- A Java Runtime Environment (JRE) installed. 

- MariaDB installed and running as a service. 

- An IDB database schema available in the MariaDB instance. Section 4 of the IDB Dataloader document gives more information about creating a new 
IDB database.

# Installing the IDB Data Loader

Installation of the pre-build binaries is described in the Data Loader documentation. 

To build from source, use the Maven pom.xml file and run the command **mvn clean install**

# Not included in Documenation

There were changes to the CSV data after the documenation for the IDB Loader was completed. In particular, a provenance.csv
file was added to the set of CSV files making up one set of spectral data. The Loader documenation describes a set of 
spectral data as containing five CSV files and does not inlucde the provenance csv file in its description of the CSV 
data. The "Preparation of the IDB Spectra" and "IDB Database Tables" documentation as well as the sample data and configuration
files, however, do include the additional file.

# Known Problmes

- Some problems and suggestions for improvements for the loader are given in the final section of the IDB Loader documentation.

- Known database problems are:

    - FLOAT fields should be DOUBLE in MariaDB to avoid round-off error.

    -  The detector_settings table should not have a FK relationship with the detector_table because the settings table is part
   of the spectrum family of tables.
