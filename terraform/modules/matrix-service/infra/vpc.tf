//
// A complete VPC implementation, including:
//
//  VPC
//  DHCP option set
//  Internet gateway
//  Route table (main)
//  One subnet in each availability zone
//  Default security group

data "aws_region" "current" {}
data "aws_availability_zones" "available" {}

locals {
   // available.count == 1 so use length(names)
  az_count =  length(data.aws_availability_zones.available.names)
  az_names =  data.aws_availability_zones.available.names
  aws_region =  data.aws_region.current.name
}

resource "null_resource" "vpc" {
  depends_on = [
    aws_vpc_dhcp_options_association.a,
    aws_internet_gateway.igw,
    aws_main_route_table_association.a,
    aws_route_table_association.a,
    aws_default_security_group.sg
  ]
}

resource "aws_vpc" "vpc" {
  cidr_block =  var.vpc_cidr_block
  enable_dns_support = true
  enable_dns_hostnames = true

  tags = {
    Name = "matrix-${var.deployment_stage}"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

resource "aws_vpc_dhcp_options" "opts" {
  domain_name          = "ec2.internal"
  domain_name_servers  = ["AmazonProvidedDNS"]

  tags = {
    Name = "matrix-${var.deployment_stage}"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

resource "aws_vpc_dhcp_options_association" "a" {
  vpc_id          =  aws_vpc.vpc.id
  dhcp_options_id =  aws_vpc_dhcp_options.opts.id
}

resource "aws_internet_gateway" "igw" {
  vpc_id =  aws_vpc.vpc.id

  tags = {
    Name = "matrix-${var.deployment_stage}"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

resource "aws_route_table" "rt" {
  vpc_id =  aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id =  aws_internet_gateway.igw.id
  }

  tags = {
    Name = "matrix-${var.deployment_stage}"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

resource "aws_main_route_table_association" "a" {
  vpc_id         =  aws_vpc.vpc.id
  route_table_id =  aws_route_table.rt.id
}

resource "aws_subnet" "sn" {
  count =  local.az_count

  availability_zone =  local.az_names[count.index]
  cidr_block        =  cidrsubnet(var.vpc_cidr_block, "4", count.index)
  vpc_id            =  aws_vpc.vpc.id
  map_public_ip_on_launch = true

  tags = {
    Name = "matrix-${var.deployment_stage}-${local.az_names[count.index]}"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

resource "aws_route_table_association" "a" {
  count =  length(aws_subnet.sn)

  subnet_id      =  aws_subnet.sn.*.id[count.index]
  route_table_id =  aws_route_table.rt.id
}

resource "aws_default_security_group" "sg" {
  vpc_id =  aws_vpc.vpc.id

  ingress {
    protocol  = -1
    self      = true
    from_port = 0
    to_port   = 0
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "matrix-${var.deployment_stage}-default"
    component = "matrix"
    deployment_stage =  var.deployment_stage
  }
}

data "aws_subnet_ids" "matrix_vpc" {
  vpc_id =  aws_vpc.vpc.id
}
