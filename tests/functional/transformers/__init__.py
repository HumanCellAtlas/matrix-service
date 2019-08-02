from matrix.common.aws.redshift_handler import TableName
from matrix.common.constants import BundleType


ETL_TEST_BUNDLES = {
    'integration': {
        BundleType.SS2: {
            '5cb665f4-97bb-4176-8ec2-1b83b95c1bc0': {
                TableName.ANALYSIS: "5f7dee36-68e8-41c8-9e5f-50f3c772176a|"
                                    "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0.2019-02-11T171739.925160Z|"
                                    "5cb665f4-97bb-4176-8ec2-1b83b95c1bc0|2019-02-11T171739.925160Z|"
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
                                "84dd5dd7-cad0-4874-a36e-d2ca7e9d1489|5f7dee36-68e8-41c8-9e5f-50f3c772176a|"
                                "e11a1b8b-c83a-419b-bbb7-dc5d214770d0|2019-02-11T171726.073959Z||3859||\n",
                TableName.EXPRESSION: "2c748259-d3a5-4a1a-9b1a-c2e0dca6fccc|ENST00000373020|TPM|92.29\n"
            },
            '343dbc5f-bfe8-4f51-b493-a508d017125c': {
                TableName.ANALYSIS: "223f7882-3ba8-4da6-af37-494aa593fcd9|"
                                    "343dbc5f-bfe8-4f51-b493-a508d017125c.2019-05-24T185218.516000Z|"
                                    "343dbc5f-bfe8-4f51-b493-a508d017125c|2019-05-24T185218.516000Z|"
                                    "smartseq2_v2.3.0|blessed",
                TableName.SPECIMEN: "942ed1bf-d638-4d6c-9953-97deb555604b|NCBITAXON:9606|Homo sapiens|HANCESTRO:0016|"
                                    "African American or Afro-Caribbean|MONDO:0011273|H syndrome|EFO:0001272|adult|"
                                    "UBERON:0002113|kidney|UBERON:0014451|tongue taste bud",
                TableName.LIBRARY_PREPARATION: "2c25e25e-6dff-4028-a649-7b454aeb175a|OBI:0000869|polyA RNA extract|"
                                               "EFO:0008931|Smart-seq2|full length|unstranded",
                TableName.PROJECT: "4556d1c7-b6a0-4829-9dfb-ab2b668eb445|integration/Smart-seq2/2019-05-24T18:04:07Z|"
                                   "SS2 1 Cell Integration Test",
                TableName.PUBLICATION: "4556d1c7-b6a0-4829-9dfb-ab2b668eb445|Study of single cells in the human body|"
                                       "10.1016/j.cell.2016.07.054",
                TableName.CONTRIBUTOR: "4556d1c7-b6a0-4829-9dfb-ab2b668eb445|Jane,,Smith|University of Washington",
                TableName.CELL: "e584b404-9937-49fe-a323-5e18511f7035|e584b404-9937-49fe-a323-5e18511f7035|"
                                "4556d1c7-b6a0-4829-9dfb-ab2b668eb445|942ed1bf-d638-4d6c-9953-97deb555604b|"
                                "2c25e25e-6dff-4028-a649-7b454aeb175a|223f7882-3ba8-4da6-af37-494aa593fcd9|"
                                "91c3a4fb-b756-4200-b1bf-87253d431f2c|2019-05-24T185543.995479Z||3859||\n",
                TableName.EXPRESSION: "e584b404-9937-49fe-a323-5e18511f7035|ENST00000373020|TPM|92.29\n"
            },
        },
        BundleType.CELLRANGER: {
            'baed2abb-bf4a-4239-a605-38c7a1129596': {
                TableName.ANALYSIS: "0603fcfc-a3eb-4442-a6bc-ed4495f0362c|"
                                    "baed2abb-bf4a-4239-a605-38c7a1129596.2019-01-07T214226.767214Z|"
                                    "baed2abb-bf4a-4239-a605-38c7a1129596|2019-01-07T214226.767214Z|"
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
                                "aee36f18-c90f-403a-81cb-c7af96185e1c|2019-01-07T214220.472672Z|"
                                "TGAGCATAGTACGATA-1|148||\n",
                TableName.EXPRESSION: "488dc8c5ce601b9833aad68b22cdae0e|ENSG00000198786|Count|2\n"
            },
        }
    },
    'staging': {
        BundleType.SS2: {
            'bffa26df-b1d4-439b-bb82-2d0a271ad0ef': {
                TableName.ANALYSIS: "7484c788-61af-403b-92a8-be3ca94d8789|"
                                    "bffa26df-b1d4-439b-bb82-2d0a271ad0ef.2019-05-24T192401.324000Z|"
                                    "bffa26df-b1d4-439b-bb82-2d0a271ad0ef|2019-05-24T192401.324000Z|"
                                    "smartseq2_v2.3.0|blessed",
                TableName.SPECIMEN: "04b3083a-5f27-43e5-bb6a-520d083fa8b8|NCBITAXON:9606|Homo sapiens|HANCESTRO:0016|"
                                    "African American or Afro-Caribbean|MONDO:0011273|H syndrome|EFO:0001272|adult|"
                                    "UBERON:0002113|kidney|UBERON:0014451|tongue taste bud",
                TableName.LIBRARY_PREPARATION: "2b91bdab-103c-45f8-a55e-761d3131221c|OBI:0000869|polyA RNA extract|"
                                               "EFO:0008931|Smart-seq2|full length|unstranded",
                TableName.PROJECT: "f6d7a66a-d8b2-4240-9673-56b19eb9c9c0|staging/Smart-seq2/2019-05-24T18:05:06Z|"
                                   "SS2 1 Cell Integration Test",
                TableName.PUBLICATION: None,
                TableName.CONTRIBUTOR: "f6d7a66a-d8b2-4240-9673-56b19eb9c9c0|John,D,Doe|EMBL-EBI",
                TableName.CELL: "891c905c-7219-4b55-9c1b-4a5d7044e626|891c905c-7219-4b55-9c1b-4a5d7044e626|"
                                "f6d7a66a-d8b2-4240-9673-56b19eb9c9c0|04b3083a-5f27-43e5-bb6a-520d083fa8b8|"
                                "2b91bdab-103c-45f8-a55e-761d3131221c|7484c788-61af-403b-92a8-be3ca94d8789|"
                                "eaa4b7d1-dd99-4f87-bc22-6fe2337720c0|2019-05-24T192722.908661Z||3859||\n",
                TableName.EXPRESSION: "891c905c-7219-4b55-9c1b-4a5d7044e626|ENST00000373020|TPM|92.29\n",
            },
        },
        BundleType.CELLRANGER: {}
    },
    'prod': {
        BundleType.SS2: {
            'ffd3bc7b-8f3b-4f97-aa2a-78f9bac93775': {
                TableName.ANALYSIS: "3ec8dd71-5631-4d03-a6fc-470217e26c85|"
                                    "ffd3bc7b-8f3b-4f97-aa2a-78f9bac93775.2019-05-14T122736.345000Z|"
                                    "ffd3bc7b-8f3b-4f97-aa2a-78f9bac93775|2019-05-14T122736.345000Z|"
                                    "smartseq2_v2.3.0|blessed",
                TableName.SPECIMEN: "79926ae9-f27d-4ebe-9646-a0188fc145af|NCBITAXON:9606|Homo sapiens|HANCESTRO:0005|"
                                    "European|PATO:0000461|normal|HSAPDV:0000091|human late adulthood stage|"
                                    "UBERON:0001264|pancreas|UBERON:0000006|islet of Langerhans",
                TableName.LIBRARY_PREPARATION: "3ab6b486-f900-4f70-ab34-98859ac5f77a|OBI:0000869|polyA RNA extract|"
                                               "EFO:0008931|Smart-seq2|full length|unstranded",
                TableName.PROJECT: "cddab57b-6868-4be4-806f-395ed9dd635a|"
                                   "Single cell transcriptome analysis of human pancreas|"
                                   "Single cell transcriptome analysis of human pancreas reveals transcriptional "
                                   "signatures of aging and somatic mutation patterns.",
                TableName.PUBLICATION: None,
                TableName.CONTRIBUTOR: "cddab57b-6868-4be4-806f-395ed9dd635a|Matthew,,Green|"
                                       "EMBL-EBI European Bioinformatics Institute",
                TableName.CELL: "3c2180aa-0aa4-411f-98dc-73ef87b447ed|3c2180aa-0aa4-411f-98dc-73ef87b447ed|"
                                "cddab57b-6868-4be4-806f-395ed9dd635a|79926ae9-f27d-4ebe-9646-a0188fc145af|"
                                "3ab6b486-f900-4f70-ab34-98859ac5f77a|3ec8dd71-5631-4d03-a6fc-470217e26c85|"
                                "6ffea35a-f721-4995-bf97-7c9fa9ea30cf|2019-05-15T003246.351336Z||4020||\n",
                TableName.EXPRESSION: "3c2180aa-0aa4-411f-98dc-73ef87b447ed|ENST00000509541|TPM|0.72\n"
            },
        },
        BundleType.CELLRANGER: {}
    }
}
