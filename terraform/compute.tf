locals {
  airflow_firewall_rules = {
    allow-airflow-ui-myip = {
      ports         = ["8080"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
    allow-iap-8080 = {
      ports         = ["8080"]
      source_ranges = ["35.235.240.0/20"]
      target_tags   = ["airflow-ui"]
    }
    allow-iap-ssh = {
      ports         = ["22"]
      source_ranges = ["35.235.240.0/20"]
      target_tags   = null
    }
    allow-ssh-from-myip = {
      ports         = ["22"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
    allow-ssh-myip-v4 = {
      ports         = ["22"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
    allow-ssh-myip-v4-2 = {
      ports         = ["22"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
    allow-ssh-myip-v4b = {
      ports         = ["22"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
    allow-ssh-myip-v4c = {
      ports         = ["22"]
      source_ranges = [var.operator_ipv4_cidr]
      target_tags   = null
    }
  }
}

resource "google_compute_instance" "airflow" {
  name                       = "airflow-vm"
  project                    = var.project_id
  zone                       = var.zone
  machine_type               = "e2-medium"
  description                = "role=airflow\nenv=portfolio"
  can_ip_forward             = false
  deletion_protection        = true
  allow_stopping_for_update  = false
  enable_display             = false
  key_revocation_action_type = "NONE"
  resource_policies          = []
  tags                       = ["airflow-ui"]

  boot_disk {
    auto_delete  = true
    device_name  = "airflow-vm"
    force_attach = false
    mode         = "READ_WRITE"

    initialize_params {
      enable_confidential_compute = false
      image                       = "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20250812"
      size                        = 30
      type                        = "pd-standard"
    }
  }

  network_interface {
    network    = data.google_compute_network.default.self_link
    subnetwork = data.google_compute_subnetwork.default.self_link
    stack_type = "IPV4_ONLY"

    access_config {
      network_tier = "STANDARD"
    }
  }

  metadata = {
    enable-osconfig = "TRUE"
    startup-script  = "#!C:/Program Files/Git/usr/bin/bash\nsystemctl enable ssh || true\nsystemctl restart ssh || true"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  confidential_instance_config {
    enable_confidential_compute = false
  }

  reservation_affinity {
    type = "ANY_RESERVATION"
  }

  service_account {
    email  = local.airflow_service_account
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [metadata]
  }
}

resource "google_compute_firewall" "airflow" {
  for_each = local.airflow_firewall_rules

  name          = each.key
  project       = var.project_id
  network       = data.google_compute_network.default.name
  direction     = "INGRESS"
  priority      = 1000
  source_ranges = each.value.source_ranges
  target_tags   = each.value.target_tags

  allow {
    protocol = "tcp"
    ports    = each.value.ports
  }

  lifecycle {
    prevent_destroy = true
  }
}
