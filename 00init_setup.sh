

#gcloud auth login

# Configure gcloud to use your project
#gcloud config set project your-actual-project-id

# export it as variable
export PROJECT_ID=$(gcloud config get-value project)
echo $PROJECT_ID

# create a VPC
# -----------------
gcloud compute networks create europe-vpc \
  --project=$PROJECT_ID \
  --subnet-mode=custom

# create a subnet
# ------------------
gcloud compute networks subnets create europe-subnet \
  --project=$PROJECT_ID \
  --network=europe-vpc \
  --region=europe-west3 \
  --range=10.0.0.0/24

echo "Creating runtime template!"
#  create a Runtime template
# ------------------
gcloud colab runtime-templates create \
  --display-name="EA_demo_RT" \
  --project=$PROJECT_ID \
  --region=europe-west3 \
  --machine-type=e2-standard-4 \
  --disk-type=PD_STANDARD \
  --disk-size-gb=20  \
  --network=projects/$PROJECT_ID/global/networks/europe-vpc \
  --subnetwork=projects/$PROJECT_ID/regions/europe-west3/subnetworks/europe-subnet \
  --idle-shutdown-timeout=600s

# create a GCS bucket (only if it doesn't exist)
# -----------------------------------------------
echo "Checking GCS bucket..."
if gcloud storage buckets describe gs://ea-demo-1raw >/dev/null 2>&1; then
    echo "GCS bucket ea-demo-1raw already exists, skipping creation"
else
    echo "Creating GCS bucket..."
    gcloud storage buckets create gs://ea-demo-1raw \
      --project=$PROJECT_ID \
      --location=europe-west3
fi

 

# Upload files to GCS bucket 
# -------------------------------------------------
echo "Uploading files to GCS bucket..."
gcloud storage cp ./resources/books.csv gs://ea-demo-1raw/ || echo "books.json not found"
gcloud storage cp ./resources/customers.json gs://ea-demo-1raw/ || echo "customers.json not found"
gcloud storage cp ./resources/sales.json gs://ea-demo-1raw/ || echo "sales.json not found"
gcloud storage cp ./resources/web_logs.txt gs://ea-demo-1raw/ || echo "web_logs.txt not found"

bq --location=europe-west3 mk --dataset --description "Demo dataset for EA project" $PROJECT_ID:EA_DEMO_RAW
bq --location=europe-west3 mk --dataset --description "Demo dataset for EA project" $PROJECT_ID:EA_DEMO_ACCESS

bq --location=europe-west3 mk --table \
  $PROJECT_ID:EA_DEMO_RAW.T_BOOKS_RAW \
  book_id:STRING,title:STRING,authors:STRING,category:STRING,publication_year:INTEGER,isbn:STRING,price:FLOAT,page_count:INTEGER,rating:FLOAT


## 1. Create T_CUSTOMERS_RAW table
bq --location=europe-west3 mk \
  --table \
  --description="Raw customers data table with all string columns" \
  EA_DEMO_RAW.T_CUSTOMERS_RAW \
  customer_id:STRING,customer_name:STRING,email:STRING,phone:STRING,address:STRING,city:STRING,state:STRING,zip_code:STRING,country:STRING,join_date:STRING,birth_year:STRING,preferred_genre:STRING,loyalty_tier:STRING

## 2. Create T_SALES_RAW table  
bq --location=europe-west3 mk \
  --table \
  --description="Raw sales data table with all string columns" \
  EA_DEMO_RAW.T_SALES_RAW \
  sale_id:STRING,customer_id:STRING,book_id:STRING,quantity:STRING,sale_date:STRING,sale_time:STRING,payment_method:STRING,discount_applied:STRING,shipping_cost:STRING

## 3. Create T_WEBLOGS_RAW table
bq --location=europe-west3 mk \
  --table \
  --description="Raw web logs data table with parsed columns (all strings)" \
  EA_DEMO_RAW.T_WEBLOGS_RAW \
  timestamp:STRING,log_level:STRING,customer_id:STRING,action:STRING,book_id:STRING



echo "Setup complete!"
