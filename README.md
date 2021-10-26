# datahem.registry
A wrapper API for AWS glue schema registry.

## Deploy
Save AWS credentials as GCP Secrets (gcp-aws-schema-secret-key, gcp-aws-schema-key-id) that you can reference from cloud functions as environment variables. Make sure the service account has access to the secrets, i.e. <project>@appspot.gserviceaccount.com has Secret Manager Secret Accessor.

Make sure cloud build service account has roles "Cloud Functions Admin" and "Service Account User".

```sh
gcloud beta functions deploy subjectVersionFn \
    --region europe-west1 \
    --trigger-http \
    --allow-unauthenticated \
    --runtime java11 \
    --entry-point org.datahem.registry.SubjectVersion \
    --memory 512MB \
    --set-env-vars AWS_REGION=eu-west-1 \
    --set-secrets=AWS_SECRET_ACCESS_KEY=projects/${PROJECT_ID}/secrets/gcp-aws-schema-secret-key:latest,AWS_ACCESS_KEY_ID=projects/${PROJECT_ID}/secrets/gcp-aws-schema-key-id:latest

```