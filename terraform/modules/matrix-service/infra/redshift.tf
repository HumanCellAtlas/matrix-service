resource "aws_redshift_cluster" "default" {
  cluster_identifier = "dcp-matrix-service-cluster-${var.deployment_stage}"
  database_name      = "matrix_service_${var.deployment_stage}"
  master_username    = "${var.redshift_username}"
  master_password    = "${var.redshift_password}"
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes    = 4
}