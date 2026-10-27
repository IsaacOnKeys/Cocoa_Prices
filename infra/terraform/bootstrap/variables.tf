variable "project_id" {
  description = "Google Cloud project containing the Cocoa Prices infrastructure."
  type        = string
  default     = "cocoa-prices-430315"
}

variable "region" {
  description = "Primary Google Cloud region."
  type        = string
  default     = "europe-west3"
}

variable "state_bucket_name" {
  description = "Globally unique bucket used by Terraform's GCS backends."
  type        = string
  default     = "cocoa-prices-430315-terraform-state"
}
