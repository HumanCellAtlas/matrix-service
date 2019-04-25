from enum import Enum


class MatrixFormat(Enum):
    """
    Supported expression matrix output formats.
    Keep up-to-date with config/matrix-api.yml (MatrixFormat)
    """
    LOOM = "loom"
    CSV = "csv"
    MTX = "mtx"


class MatrixRequestStatus(Enum):
    COMPLETE = "Complete"
    IN_PROGRESS = "In Progress"
    FAILED = "Failed"


class BundleType(Enum):
    """
    Supported bundle types
    """
    SS2 = "ss2"
    CELLRANGER = "cellranger"


MATRIX_ENV_TO_DSS_ENV = {
    'predev': "prod",
    'dev': "prod",
    'integration': "integration",
    'staging': "prod",
    'prod': "prod",
}


CREATE_QUERY_TEMPLATE = {
    'cell': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            cellkey            VARCHAR(60) NOT NULL,
            cell_suspension_id VARCHAR(60) NOT NULL,
            projectkey         VARCHAR(60) NOT NULL,
            specimenkey        VARCHAR(60) NOT NULL,
            librarykey         VARCHAR(60) NOT NULL,
            analysiskey        VARCHAR(60) NOT NULL,
            barcode            VARCHAR(32),
            genes_detected     INTEGER,
            PRIMARY KEY(cellkey),
            FOREIGN KEY(projectkey) REFERENCES project{1}(projectkey),
            FOREIGN KEY(specimenkey) REFERENCES specimen{1}(specimenkey),
            FOREIGN KEY(librarykey) REFERENCES library_preparation{1}(librarykey),
            FOREIGN KEY(analysiskey) REFERENCES analysis{1}(analysiskey))
            DISTKEY(cellkey)
            SORTKEY(cellkey, projectkey)
        ;
    """,
    'expression': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            cellkey          VARCHAR(60) NOT NULL,
            featurekey       VARCHAR(25) NOT NULL,
            exprtype         VARCHAR(10) NOT NULL,
            exrpvalue        REAL NOT NULL,
            FOREIGN KEY(cellkey) REFERENCES cell{1}(cellkey))
            DISTKEY(cellkey)
            COMPOUND SORTKEY(cellkey, featurekey)
        ;
    """,
    'feature': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
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
        CREATE {0}TABLE IF NOT EXISTS {2} (
            analysiskey         VARCHAR(60) NOT NULL,
            bundle_fqid         VARCHAR(65) NOT NULL,
            protocol            VARCHAR(40),
            awg_disposition     VARCHAR(12),
            PRIMARY KEY(analysiskey))
            DISTSTYLE ALL
            SORTKEY(analysiskey)
        ;
    """,
    'specimen': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            specimenkey                 VARCHAR(40) NOT NULL,
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
            PRIMARY KEY(specimenkey))
            DISTSTYLE ALL
            SORTKEY(specimenkey)
        ;
    """,
    'library_preparation': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
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
        CREATE {0}TABLE IF NOT EXISTS {2} (
            projectkey            VARCHAR(60) NOT NULL,
            short_name            VARCHAR(150) NOT NULL,
            title                 VARCHAR(300) NOT NULL,
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """,
    'publication': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            projectkey            VARCHAR(60) NOT NULL,
            pub_title             VARCHAR(200) NOT NULL,
            pub_doi               VARCHAR(40),
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """,
    'contributor': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            projectkey            VARCHAR(60) NOT NULL,
            cont_name             VARCHAR(150) NOT NULL,
            cont_institution      VARCHAR(150),
            PRIMARY KEY(projectkey)
        ) DISTSTYLE ALL;
    """,
    'write_lock': """
        CREATE TABLE IF NOT EXISTS {2} (
            primarykey            VARCHAR(60) NOT NULL,
            PRIMARY KEY(primarykey)
        );
    """
}
