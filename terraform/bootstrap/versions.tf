terraform {
  required_version = ">= 1.10, < 2.0"

  backend "gcs" {
    bucket = "cocoa-prices-430315-terraform-state"
    prefix = "terraform/bootstrap"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}
