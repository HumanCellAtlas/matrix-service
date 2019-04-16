from matrix.common.aws.redshift_handler import TableName
from matrix.common.constants import BundleType


ETL_TEST_BUNDLES = {
    'integration': {
        BundleType.SS2: {
            '5cb665f4-97bb-4176-8ec2-1b83b95c1bc0': {
                TableName.ANALYSIS: "5f7dee36-68e8-41c8-9e5f-50f3c772176a|"
                                    "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z|"
                                    "smartseq2_v2.2.0|blessed",
                TableName.SPECIMEN: "ca71bc0a-977d-4825-9ead-9e5741afe8e3|NCBITAXON:9606|Homo sapiens|HANCESTRO:0016|"
                                    "African American or Afro-Caribbean|MONDO:0011273|H syndrome|EFO:0001272|adult|"
                                    "UBERON:0002113|kidney|UBERON:0014451|tongue taste bud",
                TableName.LIBRARY_PREPARATION: "84dd5dd7-cad0-4874-a36e-d2ca7e9d1489|OBI:0000869|polyA RNA extract|"
                                               "EFO:0008931|Smart-seq2|full length|unstranded",
                TableName.PROJECT: "1f6aecb3-09a0-432b-bece-d2790da570d6|integration/Smart-seq2/2019-02-11T16:26:25Z|"
                                   "SS2 1 Cell Integration Test",
                TableName.PUBLICATION: "1f6aecb3-09a0-432b-bece-d2790da570d6|Study of single cells in the human body|"
                                       "10.1016/j.cell.2016.07.054",
                TableName.CONTRIBUTOR: "1f6aecb3-09a0-432b-bece-d2790da570d6|John,D,Doe|EMBL-EBI",
                TableName.CELL: "2c748259-d3a5-4a1a-9b1a-c2e0dca6fccc|2c748259-d3a5-4a1a-9b1a-c2e0dca6fccc|"
                                "1f6aecb3-09a0-432b-bece-d2790da570d6|ca71bc0a-977d-4825-9ead-9e5741afe8e3|"
                                "84dd5dd7-cad0-4874-a36e-d2ca7e9d1489|5f7dee36-68e8-41c8-9e5f-50f3c772176a||3859\n",
                TableName.EXPRESSION: "2c748259-d3a5-4a1a-9b1a-c2e0dca6fccc|ENST00000373020|TPM|92.29\n"
            },
        },
        BundleType.CELLRANGER: {
            'baed2abb-bf4a-4239-a605-38c7a1129596': {
                TableName.ANALYSIS: "0603fcfc-a3eb-4442-a6bc-ed4495f0362c|"
                                    "baed2abb-bf4a-4239-a605-38c7a1129596.2019-01-07T214226.767214Z|"
                                    "cellranger_v1.0.2|community",
                TableName.SPECIMEN: "f1bf7167-5948-4d55-9090-1f30a39fc564|NCBITAXON:9606|Homo sapiens|HANCESTRO:0005|"
                                    "European|MONDO:0001932|atrophic vulva|||UBERON:0000955|brain|UBERON:0001876|"
                                    "amygdala",
                TableName.LIBRARY_PREPARATION: "7fa4f1e6-fa10-46eb-88e8-ebdadbf3eeab|OBI:0000869|polyA RNA extract|"
                                               "EFO:0009310|10X v2 sequencing|full length|unstranded",
                TableName.PROJECT: "9080b7a6-e1e9-45e4-a68e-353cd1438a0f|Q4_DEMO-project_PRJNA248302|"
                                   "Q4_DEMO-Single cell RNA-seq of primary human glioblastomas",
                TableName.PUBLICATION: "9080b7a6-e1e9-45e4-a68e-353cd1438a0f|A title of a publication goes here.|"
                                       "10.1016/j.cell.2016.07.054",
                TableName.CONTRIBUTOR: "9080b7a6-e1e9-45e4-a68e-353cd1438a0f|John,D,Doe. |EMBL-EBI",
                TableName.CELL: "f066ea23371d725f8ec3868382ca1cc1|021d111b-4941-4e33-a2d1-8c3478f0cbd7|"
                                "9080b7a6-e1e9-45e4-a68e-353cd1438a0f|f1bf7167-5948-4d55-9090-1f30a39fc564|"
                                "7fa4f1e6-fa10-46eb-88e8-ebdadbf3eeab|0603fcfc-a3eb-4442-a6bc-ed4495f0362c|"
                                "TGAGCATAGTACGATA-1|148\n",
                TableName.EXPRESSION: "488dc8c5ce601b9833aad68b22cdae0e|ENSG00000198786|Count|2\n"
            },
        }
    },
    'staging': {
        BundleType.SS2: {},
        BundleType.CELLRANGER: {}
    },
    'prod': {
        BundleType.SS2: {
            'fff2cdde-4bd0-456f-93fc-5da18754272f': {
                TableName.ANALYSIS: "29c35608-fbb4-43da-acf8-f87357f0dba3|"
                                    "fff2cdde-4bd0-456f-93fc-5da18754272f.2019-02-01T193806.576719Z|"
                                    "smartseq2_v2.2.0|blessed",
                TableName.SPECIMEN: "31631609-4a44-4a39-a2ec-06632b3fa26d|NCBITAXON:9606|Homo sapiens|||"
                                    "PATO:0000461|normal|EFO:0001272|adult|UBERON:0002450|decidua||",
                TableName.LIBRARY_PREPARATION: "edda2708-1172-47f0-9c8b-b6771f463db1|OBI:0000869|polyA RNA extract|"
                                               "EFO:0008931|Smart-seq2|full length|unstranded",
                TableName.PROJECT: "aabbec1a-1215-43e1-8e42-6489af25c12c|Fetal/Maternal Interface|"
                                   "Reconstructing the human first trimester fetal-maternal interface "
                                   "using single cell transcriptomics",
                TableName.PUBLICATION: "aabbec1a-1215-43e1-8e42-6489af25c12c|"
                                       "Reconstructing the human first trimester fetal-maternal interface "
                                       "using single cell transcriptomics|10.1101/429589",
                TableName.CONTRIBUTOR: "aabbec1a-1215-43e1-8e42-6489af25c12c|Roser,,Vento-Tormo|"
                                       "Wellcome Trust Sanger Institute",
                TableName.CELL: "40ace1d6-bec8-4186-898b-d6aebbb1af82|40ace1d6-bec8-4186-898b-d6aebbb1af82|"
                                "aabbec1a-1215-43e1-8e42-6489af25c12c|31631609-4a44-4a39-a2ec-06632b3fa26d|"
                                "edda2708-1172-47f0-9c8b-b6771f463db1|29c35608-fbb4-43da-acf8-f87357f0dba3||6132\n",
                TableName.EXPRESSION: "40ace1d6-bec8-4186-898b-d6aebbb1af82|ENST00000371588|TPM|69.86\n"
            },
        },
        BundleType.CELLRANGER: {
            'feea80a7-ec5b-4b20-9e14-7b45676875e5': {
                TableName.ANALYSIS: "a7a93615-a8f7-4e2d-8f3f-62e79841a3f7|"
                                    "feea80a7-ec5b-4b20-9e14-7b45676875e5.2018-12-12T235115.759620Z|"
                                    "cellranger_v1.0.2|community",
                TableName.SPECIMEN: "3902d552-081a-4e9d-83a4-0f8ea0f5b74c|NCBITAXON:9606|Homo sapiens|HANCESTRO:0005|"
                                    "European|||EFO:0001272|adult|UBERON:0002405|immune system|UBERON:0002371|"
                                    "bone marrow",
                TableName.LIBRARY_PREPARATION: "6f367bb6-7fc6-4099-a053-005f38e690cb|OBI:0000869|polyA RNA extract|"
                                               "EFO:0009310|10X v2 sequencing|3 prime tag|second",
                TableName.PROJECT: "179bf9e6-5b33-4c5b-ae26-96c7270976b8|1M Immune Cells|Census of Immune Cells",
                TableName.PUBLICATION: None,
                TableName.CONTRIBUTOR: "179bf9e6-5b33-4c5b-ae26-96c7270976b8|Julia,,Waldman|Broad Institute",
                TableName.CELL: "fed3f59022cac456e769e8a063829e0e|12befbdf-9bb8-44cc-886a-623a5e656604|"
                                "179bf9e6-5b33-4c5b-ae26-96c7270976b8|3902d552-081a-4e9d-83a4-0f8ea0f5b74c|"
                                "6f367bb6-7fc6-4099-a053-005f38e690cb|a7a93615-a8f7-4e2d-8f3f-62e79841a3f7|"
                                "GATTCAGGTGCACTTA-1|634\n",
                TableName.EXPRESSION: "c78f796e986ae508373972d00f54ecbd|ENSG00000198727|Count|4\n"
            }
        }
    }
}
