packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = ">= 1.2.0"
    }
  }
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "ami_name_prefix" {
  type    = string
  default = "weft-runtime"
}

variable "weft_binary_url" {
  type        = string
  default     = ""
  description = "HTTPS URL to a linux weft binary. Leave empty when staging a local binary via build-ami.sh."
}

variable "instance_type" {
  type    = string
  default = "t3.large"
}

variable "subnet_id" {
  type        = string
  default     = ""
  description = "Optional subnet for the builder instance."
}

variable "associate_public_ip_address" {
  type    = bool
  default = true
}

locals {
  timestamp = formatdate("YYYYMMDD-hhmmss", timestamp())
  ami_name  = "${var.ami_name_prefix}-${local.timestamp}"
}

source "amazon-ebs" "weft" {
  region                      = var.region
  instance_type               = var.instance_type
  ami_name                    = local.ami_name
  ami_description             = "Hardened Weft driver/worker runtime (AL2023)"
  ssh_username                = "ec2-user"
  associate_public_ip_address = var.associate_public_ip_address
  subnet_id                   = var.subnet_id == "" ? null : var.subnet_id

  source_ami_filter {
    filters = {
      name                = "al2023-ami-*-x86_64"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
      architecture        = "x86_64"
    }
    owners      = ["137112412989"]
    most_recent = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  tags = {
    Name      = local.ami_name
    Project   = "weft"
    Component = "runtime"
  }
}

build {
  name    = "weft-runtime"
  sources = ["source.amazon-ebs.weft"]

  provisioner "shell" {
    inline = ["sudo mkdir -p /tmp/weft-files && sudo chmod 777 /tmp/weft-files"]
  }

  # Uploads bootstrap + systemd units. Optionally includes a staged `weft` binary
  # at files/weft (placed by build-ami.sh when --binary is passed).
  provisioner "file" {
    source      = "${path.root}/files/"
    destination = "/tmp/weft-files"
  }

  provisioner "shell" {
    environment_vars = [
      "WEFT_BINARY_URL=${var.weft_binary_url}",
    ]
    script          = "${path.root}/scripts/provision.sh"
    execute_command = "chmod +x {{ .Path }}; sudo -E {{ .Path }}"
  }
}
