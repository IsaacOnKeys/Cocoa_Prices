provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone

  # Existing resources should not receive a provider-generated label merely
  # because they were adopted by Terraform.
  add_terraform_attribution_label = false
}
