## cell

The main entity that most queries will want to select on is the cell, which has
its own table:

```
cellkey          VARCHAR(60) NOT NULL
projectkey       VARCHAR(60) NOT NULL
donorkey         VARCHAR(60) NOT NULL
librarykey       VARCHAR(60) NOT NULL
analysiskey      VARCHAR(60) NOT NULL
barcode          VARCHAR(32)
genes_detected   INTEGER
```

The primary key is `cellkey`. For some assay types, this has an obvious value.
For example, for assays where a single cell suspension yields a single cell,
then the id of the cell suspension can service as the `cellkey`. For
droplet-based assays on the other hand, cells are not discovered until after
secondary analysis. For those assays, `cellkey` is a hash of unique identifiers
of the cell. For example, for 10x it is the hash of (project id, donor id,
cell suspension id, lane, barcode).

The other `key` field reference other tables described below. The `barcode`
field is null for assays without barcodes. The remaining fields are calculated
cell metadata. Eventually, this should include cell QC information produced by
analysis pipelines, but currently it is just `genes_detected`, the number of
genes with non-zero expression for that cell.

## expression
```
cellkey          VARCHAR(60) NOT NULL
featurekey       VARCHAR(20) NOT NULL
exprtype         VARCHAR(10) NOT NULL
exrpvalue        REAL NOT NULL
```

Expression is stored in a table the references the `cellkey` from the `cell`
table. It also refers to the `feature` table described below that provides
information about the specific gene, transcript, or other feature.
`exprtype` currently can be `TPM` or `Count`, referring to whether `exprvalue`
is transcripts per million or a raw count.

## feature

Feature describes the genes and transcripts for which we have expression
counts.

```
featurekey       VARCHAR(20) NOT NULL
featurename      VARCHAR(40) NOT NULL
featuretype      VARCHAR(40)
chromosome       VARCHAR(40)
featurestart     INTEGER
featureend       INTEGER
isgene           BOOLEAN
```

`featurekey` is the ensembl transcript or gene id. `featurename` is the gene or
transcript name/symbol. `isgene` is TRUE when the feature is a gene rather than
a transcript. The remaining fields are pulled from ensembl's biomart.

## analysis

Analysis mostly just records the id of the analysis protocol that was used to
generate the expression counts. It also records the id of the DSS bundle from
which the expression data was retrieved.

```
analysiskey         VARCHAR(60) NOT NULL
bundle_uuid         VARCHAR(60) NOT NULL
protocol            VARCHAR(40)
awg_disposition     VARCHAR(12)
```

Finally, it records the status of the protocol wrt AWS approval. This can be
"blessed", "community", or one assumes, "cursed".

## donor_organism
```
donorkey                    VARCHAR(40) NOT NULL
genus_species_ontology      VARCHAR(40)
genus_species_label         VARCHAR(40)
ethnicity_ontology          VARCHAR(40)
ethnicity_label             VARCHAR(40)
disease_ontology            VARCHAR(40)
disease_label               VARCHAR(40)
development_stage_ontology  VARCHAR(40)
development_stage_label     VARCHAR(40)
organ_ontology              VARCHAR(40)
organ_label                 VARCHAR(40)
organ_part_ontology         VARCHAR(40)
organ_part_label            VARCHAR(40)
```

The donor_organism table mostly contains a subset of information from the
ingest metadata file of the same name. Each `_ontology` field is taken directly
from the metadata json; the `_label` fields are looked up from the EBI's
Ontology Lookup Service. This means the `ontology_label` fields in the metadata
itself are ignored.

`organ` requires special handling. Usually it comes from
`specimen_from_organism` metadata, and "organoid" json is present. Then it
comes from the organiod `model_for_organ` field, and ` (organiod)` is appended.

## library_preparation

Library preparation is similar to donor organism, but from the json that
describes information about the assay. Note that `strand` is known to contain
a lot of errors.

```
librarykey                       VARCHAR(40) NOT NULL
input_nucleic_acid_ontology      VARCHAR(40)
input_nucleic_acid_label         VARCHAR(40)
construction_approach_ontology   VARCHAR(40)
construction_approach_label      VARCHAR(40)
end_bias                         VARCHAR(20)
strand                           VARCHAR(20)
```

## project, publication, contributor

Project is referred to by the cell table. Publications and contributors have a
many-to-one relationship to projects and are in their own tables.

`project`
```
projectkey       VARCHAR(60) NOT NULL
short_name       VARCHAR(100) NOT NULL
title            VARCHAR(300) NOT NULL
```

`publication`
```
projectkey              VARCHAR(60) NOT NULL
pub_title               VARCHAR(200) NOT NULL
pub_doi                 VARCHAR(40)
```

`contributor`
```
projectkey             VARCHAR(60) NOT NULL
cont_name              VARCHAR(150) NOT NULL
cont_institution       VARCHAR(150)
```
