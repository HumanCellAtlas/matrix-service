from enum import Enum


class MatrixFormat(Enum):
    """
    Supported expression matrix output formats.
    Keep up-to-date with config/matrix-api.yml (MatrixFormat)
    """
    LOOM = "loom"
    CSV = "csv"
    MTX = "mtx"


class MatrixFeature(Enum):
    """Supported expression matrix features."""

    GENE = "gene"
    TRANSCRIPT = "transcript"


class MatrixRequestStatus(Enum):
    COMPLETE = "Complete"
    IN_PROGRESS = "In Progress"
    FAILED = "Failed"
    EXPIRED = "Expired"


class BundleType(Enum):
    """
    Supported bundle types
    """
    SS2 = "ss2"


class GenusSpecies(Enum):
    """Supported genera/species"""
    HUMAN = "Homo sapiens"
    MOUSE = "Mus musculus"


class MetadataSchemaName(Enum):
    PROJECT = "project"
    LIBRARY_PREPARATION_PROTOCOL = "library_preparation_protocol"
    ANALYSIS_PROTOCOL = "analysis_protocol"
    SPECIMEN_FROM_ORGANISM = "specimen_from_organism"
    DONOR_ORGANISM = "donor_organism"
    CELL_LINE = "cell_line"
    CELL_SUSPENSION = "cell_suspension"
    ORGANOID = "organoid"


DEFAULT_FIELDS = ["cell.cell_suspension_id", "cell.genes_detected", "cell.file_uuid",
                  "cell.file_version", "cell.total_umis", "cell.emptydrops_is_cell",
                  "cell.barcode", "specimen.*", "library_preparation.*", "project.*",
                  "analysis.*"]


DEFAULT_FEATURE = MatrixFeature.GENE.value


MATRIX_ENV_TO_DSS_ENV = {
    'predev': "prod",
    'dev': "prod",
    'integration': "integration",
    'staging': "staging",
    'prod': "prod",
}


SUPPORTED_METADATA_SCHEMA_VERSIONS = {
    MetadataSchemaName.PROJECT: {
        'max_major': 14,
        'max_minor': 1,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.LIBRARY_PREPARATION_PROTOCOL: {
        'max_major': 6,
        'max_minor': 2,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.ANALYSIS_PROTOCOL: {
        'max_major': 9,
        'max_minor': 1,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.SPECIMEN_FROM_ORGANISM: {
        'max_major': 10,
        'max_minor': 4,
        'min_major': 9,
        'min_minor': 0
    },
    MetadataSchemaName.DONOR_ORGANISM: {
        'max_major': 15,
        'max_minor': 5,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.CELL_LINE: {
        'max_major': 14,
        'max_minor': 5,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.CELL_SUSPENSION: {
        'max_major': 13,
        'max_minor': 3,
        'min_major': 1,
        'min_minor': 0
    },
    MetadataSchemaName.ORGANOID: {
        'max_major': 11,
        'max_minor': 3,
        'min_major': 1,
        'min_minor': 0
    }
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
            file_uuid          VARCHAR(60) NOT NULL,
            file_version       VARCHAR(30) NOT NULL,
            barcode            VARCHAR(32),
            genes_detected     INTEGER,
            total_umis         INTEGER,
            emptydrops_is_cell BOOLEAN,
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
            featuretype_10x  VARCHAR(20),
            featuretype      VARCHAR(40),
            chromosome       VARCHAR(40),
            featurestart     INTEGER,
            featureend       INTEGER,
            isgene           BOOLEAN,
            genus_species    VARCHAR(25) NOT NULL,
            PRIMARY KEY(featurekey))
            DISTSTYLE ALL
            SORTKEY(featurekey)
        ;
    """,
    'analysis': """
        CREATE {0}TABLE IF NOT EXISTS {2} (
            analysiskey         VARCHAR(60) NOT NULL,
            bundle_fqid         VARCHAR(65) NOT NULL,
            bundle_uuid         VARCHAR(40) NOT NULL,
            bundle_version      VARCHAR(30) NOT NULL,
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
            disease_label               VARCHAR(50),
            development_stage_ontology  VARCHAR(40),
            development_stage_label     VARCHAR(40),
            organ_ontology              VARCHAR(40),
            organ_label                 VARCHAR(100),
            organ_parts_ontology        VARCHAR(40),
            organ_parts_label           VARCHAR(100),
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
    'file_uuid': 'file_uuid',
    'file_version': 'file_version',
    'barcode': 'barcode',
    'total_umis': 'total_umis',
    'emptydrops_is_cell': 'emptydrops_is_cell',
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
    'organ_parts_ontology': 'derived_organ_parts_ontology',
    'organ_parts_label': 'derived_organ_parts_label',
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
    'bundle_uuid': 'bundle_uuid',
    'bundle_version': 'bundle_version',
    'protocol': 'analysis_protocol.protocol_core.protocol_id',
    'awg_disposition': 'analysis_working_group_approval_status'
}

METADATA_FIELD_TO_TABLE_COLUMN = {v: k for k, v in TABLE_COLUMN_TO_METADATA_FIELD.items()}

METADATA_FIELD_TO_TYPE = {k: "categorical" for k in METADATA_FIELD_TO_TABLE_COLUMN}
METADATA_FIELD_TO_TYPE["genes_detected"] = "numeric"
METADATA_FIELD_TO_TYPE["total_umis"] = "numeric"

TABLE_COLUMN_TO_TABLE = {
    'cell_suspension_id': 'cell',
    'genes_detected': 'cell',
    'file_uuid': 'cell',
    'file_version': 'cell',
    'total_umis': 'cell',
    'barcode': 'cell',
    'emptydrops_is_cell': 'cell',
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
    'organ_parts_ontology': 'specimen',
    'organ_parts_label': 'specimen',
    'librarykey': 'library_preparation',
    'input_nucleic_acid_ontology': 'library_preparation',
    'input_nucleic_acid_label': 'library_preparation',
    'construction_approach_ontology': 'library_preparation',
    'construction_approach_label': 'library_preparation',
    'end_bias': 'library_preparation',
    'strand': 'library_preparation',
    'projectkey': 'project',
    'short_name': 'project',
    'title': 'project',
    'analysiskey': 'analysis',
    'bundle_fqid': 'analysis',
    'bundle_uuid': 'analysis',
    'bundle_version': 'analysis',
    'protocol': 'analysis',
    'awg_disposition': 'analysis'
}

# Filters that specify the genus and species of the cells.
GENUS_SPECIES_FILTERS = [
    'specimen_from_organism.genus_species.ontology',
    'specimen_from_organism.genus_species.ontology_label'
]

FORMAT_DETAIL = {
    MatrixFormat.LOOM.value: """
<h2>HCA Matrix Service Loom Output</h2>

<p>The Loom-formatted output from the matrix service contains a Loom file with the
cells and metadata fields specified in the query.
The Loom format is documented more fully, along with code samples,
<a href="https://linnarssonlab.org/loompy/index.html">here</a>.</p>

<p>Per Loom
<a href="https://linnarssonlab.org/loompy/conventions/index.html">conventions</a>, columns
in the loom-formatted expression matrix represent cells, and rows represent
genes. The column and row attributes follow Loom conventions where applicable
as well: <code>CellID</code> uniquely identifies a cell, <code>Gene</code> is a gene name, and
<code>Accession</code> is an ensembl gene id.</p>

<p>Descriptions of the remaining metadata fields are available at the
<a href="https://prod.data.humancellatlas.org/metadata">HCA Data Browser</a>.</p>Loom file format,
see loompy.org
""",
    MatrixFormat.CSV.value: """
<h2>HCA Matrix Service CSV Output</h2>
<p>The csv-formatted output from the matrix service is a zip archive that contains three files:</p>
<table class="table table-striped table-bordered">
<thead>
<tr>
<th>Filename</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>&lt;directory_name&gt;/expression.csv</td>
<td>Expression values</td>
</tr>
<tr>
<td>&lt;directory_name&gt;/cells.csv</td>
<td>Cell metadata</td>
</tr>
<tr>
<td>&lt;directory_name&gt;/genes.csv</td>
<td>Gene (or transcript) metadata</td>
</tr>
</tbody>
</table>
<h3><code>expression.csv</code></h3>
<p>The first row is the header, and the first entry in the header is <code>cellkey</code>.
This is a unique identifier for the cell and is present in both the expression csv and cell
metadata csv. The remaining header are Ensembl IDs for the genes (or depending on the request,
transcripts).</p>

<p>The remaining rows each contain all the expression values for a cell, so cells are rows and
genes are columns. The expression values are meant to be a "raw" count, so for SmartSeq2
experiments, this is the <code>expected_count</code> field from
<a href="http://deweylab.biostat.wisc.edu/rsem/rsem-calculate-expression.html#output">RSEM
output</a>.</p>

<p>For 10x experiments analyzed with Cell Ranger, this is read from the
<code>matrix.mtx</code> file that Cell Ranger produces as its filtered feature-barcode
matrix.</p>

<h3><code>cells.csv</code></h3>
<p>The cell metadata table is oriented like the expression table, where each row represents a cell.
Each column is a different metadata field. Descriptions of some of the metadata fields can be
found at the <a href="https://prod.data.humancellatlas.org/metadata">HCA Data Browser</a>.
Additional fields, <code>genes_detected</code> for example, are calculated during secondary
analysis. Full descriptions of those fields are forthcoming.</p>

<h3><code>genes.csv</code></h3>
<p>The gene metadata contains basic information about the genes in the count matrix. Each row is a
gene, and each row corresponds to a column in the expression csv. Note that <code>featurename</code> is not
unique.</p>
""",
    MatrixFormat.MTX.value: """
<h2>HCA Matrix Service MTX Output</h2>
<p>The mtx-formatted output from the matrix service is a zip archive that contains
three files:</p>
<table class="table table-striped table-bordered">
<thead>
<tr>
<th>Filename</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>&lt;directory_name&gt;/matrix.mtx.gz</td>
<td>Expression values</td>
</tr>
<tr>
<td>&lt;directory_name&gt;/cells.tsv.gz</td>
<td>Cell metadata</td>
</tr>
<tr>
<td>&lt;directory_name&gt;/genes.tsv.gz</td>
<td>Gene (or transcript) metadata</td>
</tr>
</tbody>
</table>

<h3><code>matrix.mtx.gz</code></h3>
<p>This file contains expression values in the
<a href="https://math.nist.gov/MatrixMarket/formats.html">matrix market exchange format</a>.
This is a sparse format where only the non-zero expression values are recorded. The
columns in this file correspond to cells, and the rows correspond to genes or transcripts.
The column and row indices are aligned with the rows of the cell and gene metadata TSVs,
respectively.</p>

<p>The expression values are meant to be a "raw" count, so for SmartSeq2 experiments, this
is the <code>expected_count</code> field from
<a href="http://deweylab.biostat.wisc.edu/rsem/rsem-calculate-expression.html#output">RSEM
output</a>. For 10x experiments analyzed with Cell Ranger, this is read from the
<code>matrix.mtx</code> file that Cell Ranger produces as its filtered feature-barcode matrix.</p>

<h3><code>cells.tsv.gz</code></h3>
<p>Each row of the cell metadata table represents a cell, and each column is a different metadata
field. Descriptions of some of the metadata fields can be found at the
<a href="https://prod.data.humancellatlas.org/metadata">HCA Data Browser</a>. Additional
fields, <code>genes_detected</code> for example, are calculated during secondary analysis.
Full descriptions of those fields are forthcoming.</p>

<h3><code>genes.tsv.gz</code></h3>
<p>The gene metadata contains basic information about the genes in the count matrix.
Each row is a gene, and each row corresponds to the same row in the expression mtx file.
Note that <code>featurename</code> is not unique.</p>
"""
}

FIELD_DETAIL = {
    "cell_suspension.provenance.document_id":
        "Unique identifier for the suspension of cells or nuclei derived from the collected or cultured specimen.",
    "genes_detected":
        "Count of genes with a non-zero count.",
    "total_umis":
        "Count of UMIs (for droplet-based assays).",
    "barcode": "Cell barcode (for droplet-based assays).",
    "emptydrops_is_cell":
        "Cell call from emptyDrops run with default parameters (for droplet-based assays).",
    "specimen_from_organism.provenance.document_id":
        "Unique identified for the specimen that was collected from the donor organism.",
    "specimen_from_organism.genus_species.ontology":
        "An ontology term identifier in the form prefix:accession for the species to which the donor organism belongs.",
    "specimen_from_organism.genus_species.ontology_label":
        "The preferred label for the specimen_from_organism.genus_species.ontoloty ontology term",
    "donor_organism.human_specific.ethnicity.ontology":
        "An ontology term identifier in the form prefix:accession for the ethnicity of a human donor.",
    "donor_organism.human_specific.ethnicity.ontology_label":
        "The preferred label for the donor_organism.human_specific.ethnicity.ontology term.",
    "donor_organism.diseases.ontology":
        "An ontology term identifier in the form prefix:accession for a known disease of the organism.",
    "donor_organism.diseases.ontology_label":
        "The preferred label for the donor_organism.diseases.ontology term",
    "donor_organism.development_stage.ontology":
        "An ontology term identifier in the form prefix:accession for the development stage of the donor organism.",
    "donor_organism.development_stage.ontology_label":
        "The preferred label for the donor_organism.development_stage.ontology term",
    "derived_organ_ontology":
        ("An ontology term identifier in the form prefix:accession for the organ that the biomaterial came from. For "
         "cell lines and organoids, the term is for the organ model."),
    "derived_organ_label":
        "The preferred label for the derived_organ_ontology term.",
    "derived_organ_parts_ontology":
        ("An ontology term identifier in the form of prefix:accession for the specific part of the organ "
         "that the biomaterial came from. For cell lines and organoids, the term refers to the organ model."),
    "derived_organ_parts_label":
        "The preferred label for the derived_organ_parts_ontology term.",
    "library_preparation_protocol.provenance.document_id":
        "Unique identifier for how a sequencing library was prepared.",
    "library_preparation_protocol.input_nucleic_acid_molecule.ontology":
        ("An ontology term identifier in the form prefix:accession for the starting nucleic acid molecule "
         "isolated for sequencing."),
    "library_preparation_protocol.input_nucleic_acid_molecule.ontology_label":
        "The preferred label for the library_preparation_protocol.input_nucleic_acid_molecule.ontology_label",
    "library_preparation_protocol.library_construction_method.ontology":
        ("An ontology term identifier in the form prefix:accession for the general method for "
         "sequencing library construction."),
    "library_preparation_protocol.library_construction_method.ontology_label":
        "The preferred label for the library_preparation_protocol.library_construction_method.ontology_label",
    "library_preparation_protocol.end_bias":
        "The type of tag or end bias the library has.",
    "library_preparation_protocol.strand":
        "Library strandedness.",
    "project.provenance.document_id":
        "Unique identifier for overall project.",
    "project.project_core.project_short_name":
        "A short name for the project.",
    "project.project_core.project_title":
        "An official title for the project.",
    "analysis_protocol.provenance.document_id":
        "Unique identifier for the secondary analysis protocol.",
    "dss_bundle_fqid":
        "Fully-qualified identifier for the source bundle in the HCA Data Storage System.",
    "bundle_uuid":
        ("The UUID for the source bundle in the HCA Data Storage System. This field corresponds to the "
         "bundle_uuid field in HCA metadata TSV files."),
    "bundle_version":
        ("The version for the source bundle in the HCA Data Storage System. This field corresponds to the "
         "bundle_version field in HCA metadata TSV files."),
    "file_uuid":
        ("The UUID for one of this cell's source files. This field corresponds to the "
         "file_uuid field in HCA metadata TSV files."),
    "file_version":
        ("The version for one of this cell's source files. This field corresponds to the "
         "file_version field in HCA metadata TSV files."),
    "analysis_protocol.protocol_core.protocol_id":
        "A unique ID for the secondary analysis protocol.",
    "analysis_working_group_approval_status":
        "Whether the secondary analysis protocol has been reviewed and approved the HCA Analysis Working Group."
}

# Keep FIELD_DETAIL in sync with METADATA_FIELD_TO_TABLE_COLUMN
for key in METADATA_FIELD_TO_TABLE_COLUMN:
    if key not in FIELD_DETAIL:
        FIELD_DETAIL[key] = "No description available, but consult https://prod.data.humancellatlas.org/metadata"
for key in FIELD_DETAIL:
    if key not in METADATA_FIELD_TO_TABLE_COLUMN:
        FIELD_DETAIL.pop(key)

FILTER_DETAIL = FIELD_DETAIL

FEATURE_DETAIL = {
    MatrixFeature.GENE.value: "Genes from the GENCODE v27 comprehensive annotation.",
    MatrixFeature.TRANSCRIPT.value: (
        "Transcripts. from the GENCODE v27 comprehensive annotation. "
        "NOTE: Not all assay types have transcript information available")
}
