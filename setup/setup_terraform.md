## Terraform

Terraform is used to create and manage the GCP infrastructure.
The Terraform configuration files are located in the [terraform folder](terraform/).

There are 3 files:
* [main.tf](terraform/main.tf) - the configuration file;
* [variables.tf](terraform/variables.tf) - holds the variables;
* [.terraform-version](terraform/.terraform-version) - holds the Terraform version used.

How to run:
1. Run `terraform init` command to initialize the configuration;
2. Use `terraform plan` to compare previous local changes with a remote state;
3. Apply the changes to the cloud with `terraform apply`. Confirm the chances to be made with a 'yes'.

For removing the infrastructure from GCP:
* Run `terraform destroy`.