default: plan

init:
	terraform init

plan:
	terraform plan -detailed-exitcode

apply:
	terraform apply --backup=-

import:
	# These resources are shared between deployments in the same account
	- terraform import module.matrix_service_infra.aws_iam_role.AWSBatchServiceRole AWSBatchServiceRole
	- terraform import module.matrix_service_infra.aws_iam_role.AmazonEC2SpotFleetRole AmazonEC2SpotFleetRole
	- terraform import module.matrix_service_infra.aws_iam_role.ecsInstanceRole ecsInstanceRole
	- terraform import module.matrix_service_infra.aws_iam_instance_profile.ecsInstanceRole ecsInstanceRole
	- terraform import module.matrix_service_infra.aws_iam_service_linked_role.AWSServiceRoleForEC2Spot arn:aws:iam::109067257620:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot
	- terraform import module.matrix_service_infra.aws_iam_service_linked_role.AWSServiceRoleForEC2SpotFleet arn:aws:iam::109067257620:role/aws-service-role/spotfleet.amazonaws.com/AWSServiceRoleForEC2SpotFleet
