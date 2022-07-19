## GCP

You need a GCP account for this project. The setup of an account is outside of the scope of this document, but you can always look for the Free Trial.

High-level view of the main steps:
1. Create a new project. The Project ID will be needed across this repo.
2. Configure a service account and download service-account-keys (.json)  
3. Set at least the following permissions and IAM roles for the service account: 
* Viewer
* Storage Admin
* Storage Object Admin
* BigQuery Admin
* DataProc Administrator (for Dataproc)
* Service Account User(for Dataproc)
4. Enable the APIs for the project
5. Download [SDK](https://cloud.google.com/sdk/docs/quickstart) for local setup
6. Set a environment variable to point to the downloaded GCP keys in step 2:
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   
   # Refresh token/session, and verify authentication
   gcloud auth application-default login
   ```

More detail including videos for these instructions can be found in the [bootcamp Github](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp).