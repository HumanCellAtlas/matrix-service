CREATE_TABLE_QUERY = {
    'cell': """
        CREATE TABLE cell (
            cellkey          VARCHAR(60) NOT NULL,
            projectkey       VARCHAR(60) NOT NULL,
            donorkey         VARCHAR(60) NOT NULL,
            librarykey       VARCHAR(60) NOT NULL,
            analysiskey      VARCHAR(60) NOT NULL,
            barcode          VARCHAR(32),
            genes_detected   INTEGER,
            PRIMARY KEY(cellkey),
            FOREIGN KEY(projectkey) REFERENCES project(projectkey),
            FOREIGN KEY(donorkey) REFERENCES donor_organism(donorkey),
            FOREIGN KEY(librarykey) REFERENCES library_preparation(librarykey),
            FOREIGN KEY(analysiskey) REFERENCES analysis(analysiskey))
            DISTKEY(cellkey)
            SORTKEY(cellkey, projectkey)
        ;
    """,
    'expression': """
        CREATE TABLE expression (
            cellkey          VARCHAR(60) NOT NULL,
            featurekey       VARCHAR(25) NOT NULL,
            exprtype         VARCHAR(10) NOT NULL,
            exrpvalue        REAL NOT NULL,
            FOREIGN KEY(cellkey) REFERENCES cell(cellkey))
            DISTKEY(cellkey)
            COMPOUND SORTKEY(cellkey, featurekey)
        ;
    """,
    'feature': """
        CREATE TABLE feature (
            featurekey       VARCHAR(25) NOT NULL,
            featurename      VARCHAR(40) NOT NULL,
            featuretype      VARCHAR(40),
            chromosome       VARCHAR(40),
            featurestart     INTEGER,
            featureend       INTEGER,
            isgene           BOOLEAN,
            PRIMARY KEY(featurekey))
            DISTSTYLE ALL
            SORTKEY(featurekey)
        ;
    """,
    'analysis': """
        CREATE TABLE analysis (
            analysiskey         VARCHAR(60) NOT NULL,
            bundle_fqid         VARCHAR(65) NOT NULL,
            protocol            VARCHAR(40),
            awg_disposition     VARCHAR(12),
            PRIMARY KEY(analysiskey))
            DISTSTYLE ALL
            SORTKEY(analysiskey)
        ;
    """,
    'donor_organism': """
        CREATE TABLE donor_organism (
            donorkey           VARCHAR(40) NOT NULL,
            genus_species_ontology      VARCHAR(40),
            genus_species_label         VARCHAR(40),
            ethnicity_ontology          VARCHAR(40),
            ethnicity_label             VARCHAR(40),
            disease_ontology            VARCHAR(40),
            disease_label               VARCHAR(40),
            development_stage_ontology  VARCHAR(40),
            development_stage_label     VARCHAR(40),
            organ_ontology              VARCHAR(40),
            organ_label                 VARCHAR(40),
            organ_part_ontology         VARCHAR(40),
            organ_part_label            VARCHAR(40),
            PRIMARY KEY(donorkey))
            DISTSTYLE ALL
            SORTKEY(donorkey)
        ;
    """,
    'library_preparation': """
        CREATE TABLE library_preparation (
            librarykey                       VARCHAR(40) NOT NULL,
            input_nucleic_acid_ontology      VARCHAR(40),
            input_nucleic_acid_label         VARCHAR(40),
            construction_approach_ontology   VARCHAR(40),
            construction_approach_label      VARCHAR(40),
            end_bias                         VARCHAR(20),
            strand                           VARCHAR(20),
            PRIMARY KEY(librarykey))
            DISTSTYLE ALL
            SORTKEY(librarykey)
        ;
    """,
    'project': """
        CREATE TABLE project (
            projectkey            VARCHAR(60) NOT NULL,
            short_name            VARCHAR(100) NOT NULL,
            title                 VARCHAR(300) NOT NULL,
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """,
    'publication': """
        CREATE TABLE publication (
            projectkey            VARCHAR(60) NOT NULL,
            pub_title             VARCHAR(200) NOT NULL,
            pub_doi               VARCHAR(40),
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """,
    'contributor': """
        CREATE TABLE contributor (
            projectkey            VARCHAR(60) NOT NULL,
            cont_name             VARCHAR(150) NOT NULL,
            cont_institution      VARCHAR(150),
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """
}
