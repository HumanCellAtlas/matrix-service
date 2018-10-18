# Terraform resource to manage default vpc. Imported via terraform import module.matrix_service_infra.aws_default_vpc.default vpc-3aa13b43

resource "aws_default_vpc" "default" {
    tags {
        Name = "Default VPC"
        # We are using the default vpc. We have to keep this tag to not override tag required by allspark
        "kubernetes.io/cluster/allspark" = "shared"
    }
}

data "aws_subnet_ids" "default" {
  vpc_id = "${aws_default_vpc.default.id}"
}

data "aws_subnet" "default" {
  count = "${length(data.aws_subnet_ids.default.ids)}"
  id = "${data.aws_subnet_ids.default.ids[count.index]}"
}
