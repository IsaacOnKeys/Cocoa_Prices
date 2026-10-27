variable "project_id" {
  description = "Google Cloud project containing the Cocoa Prices infrastructure."
  type        = string
  default     = "cocoa-prices-430315"
}

variable "project_number" {
  description = "Numeric Google Cloud project identifier."
  type        = string
  default     = "494039866722"
}

variable "region" {
  description = "Primary Google Cloud region."
  type        = string
  default     = "europe-west3"
}

variable "zone" {
  description = "Zone containing the Airflow VM."
  type        = string
  default     = "europe-west3-c"
}

variable "operator_ipv4_cidr" {
  description = "Private operator IPv4 CIDR currently allowed by the legacy Airflow firewall rules."
  type        = string

  validation {
    condition     = can(cidrhost(var.operator_ipv4_cidr, 0))
    error_message = "operator_ipv4_cidr must be a valid IPv4 CIDR, for example 203.0.113.10/32."
  }
}
