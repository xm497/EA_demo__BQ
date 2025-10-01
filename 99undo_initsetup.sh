# Cleanup script to undo the gcloud setup

# Set PROJECT_ID variable
export PROJECT_ID=$(gcloud config get-value project)
echo "Using project: $PROJECT_ID"

echo "Starting cleanup process..."

# Delete the Runtime template
# ---------------------------
echo "Deleting runtime template..."
gcloud colab runtime-templates delete EA_demo_RT \
  --project=$PROJECT_ID \
  --region=europe-west3 \
  --quiet

# Delete the subnet
# -----------------
echo "Deleting subnet..."
gcloud compute networks subnets delete europe-subnet \
  --project=$PROJECT_ID \
  --region=europe-west3 \
  --quiet

# Delete the VPC network
# ----------------------
echo "Deleting VPC network..."
gcloud compute networks delete europe-vpc \
  --project=$PROJECT_ID \
  --quiet

# Delete the GCS bucket
# ---------------------
echo "Deleting GCS bucket..."
gcloud storage rm --recursive gs://ea-demo-1raw || echo "Bucket deletion failed or doesn't exist"

echo "Deleting BigQuery table..."
bq rm -f -t $PROJECT_ID:EA_DEMO.T_BOOKS_RAW || echo "table deletion failed or doesn't exist"
bq rm -f -t $PROJECT_ID:EA_DEMO.T_CUSTOMERS_RAW || echo "table deletion failed or doesn't exist"
bq rm -f -t $PROJECT_ID:EA_DEMO.T_SALES_RAW || echo "table deletion failed or doesn't exist"
bq rm -f -t $PROJECT_ID:EA_DEMO.T_WEBLOGS_RAW || echo "table deletion failed or doesn't exist"
echo "Deleting BigQuery dataset..."
bq rm -f -d $PROJECT_ID:EA_DEMO || echo "dataset deletion failed or doesn't exist"

echo "Cleanup complete!"
echo "All resources have been removed."
