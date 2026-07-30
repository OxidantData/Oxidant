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

variable "architecture" {
  type        = string
  default     = "arm64"
  description = "AMI / binary arch: arm64 (SF100 Graviton c6g/m8g) or x86_64."
  validation {
    condition     = contains(["arm64", "x86_64"], var.architecture)
    error_message = "Architecture must be arm64 or x86_64."
  }
}

variable "instance_type" {
  type        = string
  default     = ""
  description = "Builder instance type. Empty → t4g.large (arm64) or t3.large (x86_64)."
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
  timestamp     = formatdate("YYYYMMDD-hhmmss", timestamp())
  ami_name      = "${var.ami_name_prefix}-${var.architecture}-${local.timestamp}"
  instance_type = var.instance_type != "" ? var.instance_type : (
    var.architecture == "arm64" ? "t4g.large" : "t3.large"
  )
  # AL2023 naming: arm64 images use "al2023-ami-*-arm64", x86 use "*-x86_64".
  ami_name_glob = "al2023-ami-*-${var.architecture}"
}

source "amazon-ebs" "weft" {
  region                      = var.region
  instance_type               = local.instance_type
  ami_name                    = local.ami_name
  ami_description             = "Hardened Weft driver/worker runtime (AL2023 ${var.architecture})"
  ssh_username                = "ec2-user"
  associate_public_ip_address = var.associate_public_ip_address
  subnet_id                   = var.subnet_id == "" ? null : var.subnet_id

  source_ami_filter {
    filters = {
      name                = local.ami_name_glob
      root-device-type    = "ebs"
      virtualization-type = "hvm"
      architecture        = var.architecture
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
    Name         = local.ami_name
    Project      = "weft"
    Component    = "runtime"
    Architecture = var.architecture
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
