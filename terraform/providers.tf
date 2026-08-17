provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone

  # Keep labels explicitly controlled by this configuration.
  add_terraform_attribution_label = false
}
