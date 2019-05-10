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
    'staging': "staging",
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

# Map from internal matrix service column names to the names used in the
# project tsv and hence the API surface.
TABLE_COLUMN_TO_METADATA_FIELD = {
    'cell_suspension_id': 'cell_suspension.provenance.document_id',
    'genes_detected': 'genes_detected',
    'specimenkey': 'specimen_from_organism.provenance.document_id',
    'genus_species_ontology': 'specimen_from_organism.genus_species.ontology',
    'genus_species_label': 'specimen_from_organism.genus_species.ontology_label',
    'ethnicity_ontology': 'donor_organism.human_specific.ethnicity.ontology',
    'ethnicity_label': 'donor_organism.human_specific.ethnicity.ontology_label',
    'disease_ontology': 'donor_organism.diseases.ontology',
    'disease_label': 'donor_organism.diseases.ontology_label',
    'development_stage_ontology': 'donor_organism.development_stage.ontology',
    'development_stage_label': 'donor_organism.development_stage.ontology_label',
    'organ_ontology': 'derived_organ_ontology',
    'organ_label': 'derived_organ_label',
    'organ_part_ontology': 'derived_organ_part_ontology',
    'organ_part_label': 'derived_organ_part_label',
    'librarykey': 'library_preparation_protocol.provenance.document_id',
    'input_nucleic_acid_ontology': 'library_preparation_protocol.input_nucleic_acid_molecule.ontology',
    'input_nucleic_acid_label': 'library_preparation_protocol.input_nucleic_acid_molecule.ontology_label',
    'construction_approach_ontology': 'library_preparation_protocol.library_construction_method.ontology',
    'construction_approach_label': 'library_preparation_protocol.library_construction_method.ontology_label',
    'end_bias': 'library_preparation_protocol.end_bias',
    'strand': 'library_preparation_protocol.strand',
    'projectkey': 'project.provenance.document_id',
    'short_name': 'project.project_core.project_short_name',
    'title': 'project.project_core.project_title',
    'analysiskey': 'analysis_protocol.provenance.document_id',
    'bundle_fqid': 'dss_bundle_fqid',
    'protocol': 'analysis_protocol.protocol_core.protocol_id',
    'awg_disposition': 'analysis_working_group_approval_status'
}

METADATA_FIELD_TO_TABLE_COLUMN = {v: k for k, v in TABLE_COLUMN_TO_METADATA_FIELD.items()}

METADATA_FIELD_TO_TYPE = {k: ("categorical" if k != "genes_detected" else "numeric")
                          for k in METADATA_FIELD_TO_TABLE_COLUMN}

TABLE_COLUMN_TO_TABLE = {
    'cell_suspension_id': 'cell',
    'genes_detected': 'cell',
    'specimenkey': 'specimen',
    'genus_species_ontology': 'specimen',
    'genus_species_label': 'specimen',
    'ethnicity_ontology': 'specimen',
    'ethnicity_label': 'specimen',
    'disease_ontology': 'specimen',
    'disease_label': 'specimen',
    'development_stage_ontology': 'specimen',
    'development_stage_label': 'specimen',
    'organ_ontology': 'specimen',
    'organ_label': 'specimen',
    'organ_part_ontology': 'specimen',
    'organ_part_label': 'specimen',
    'librarykey': 'library_preparation',
    'input_nucleic_acid_ontology': 'library_preparation',
    'input_nucleic_acid_label': 'library_preparation',
    'construction_approach_ontology': 'library_preparation',
    'construction_approach_label': 'library_preparation',
    'end_bias': 'library_preparation',
    'strand': 'library_preparation',
    'projectkey': 'project.',
    'short_name': 'project',
    'title': 'project',
    'analysiskey': 'analysis',
    'bundle_fqid': 'analysis',
    'protocol': 'analysis',
    'awg_disposition': 'analysis'
}

FORMAT_DETAIL = {
    MatrixFormat.LOOM.value: "Loom file format, see loompy.org",
    MatrixFormat.CSV.value: ("Zip archive of expression values in a cell-by-gene "
                             "CSV file, and gene and cell metadata in two separate "
                             "CSV files."),
    MatrixFormat.MTX.value: ("Zip archive of expression values in a Matrix Market "
                             "Exchange formatted file, and gene and cell metadata in "
                             "two separate TSV files. See "
                             "https://math.nist.gov/MatrixMarket/formats.html for "
                             "futher details.")
}

FIELD_DETAIL = {
    k: "Just what it sounds like." for k in METADATA_FIELD_TO_TABLE_COLUMN}

FILTER_DETAIL = {
    k: "Just what it sounds like." for k in METADATA_FIELD_TO_TABLE_COLUMN}

FEATURE_DETAIL = {
    "gene": "Genes from the GENCODE v27 comprehensive annotation.",
    "transcript": ("Transcripts. from the GENCODE v27 comprehensive annotation. "
                   "NOTE: Not all assay types have transcript information available")
}
