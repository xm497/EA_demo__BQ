## Overview of Setup
The below document will list the overall setup scripts for better understanding of EA Online store

There are mainly 2 steps for the setup, one which run manually and then by script.
To run the manual script, we need to open the dev portal (https://devportal.deutsche-boerse.de/), Then Click on GCP Garage

<img width="645" height="409" alt="image" src="https://github.com/user-attachments/assets/61779bfc-818e-4709-bd00-1b7719544f83" />

Then click on the listed project. and also the workstation.
Please wait some time to boot the workstation

<img width="645" height="218" alt="image" src="https://github.com/user-attachments/assets/b32d42a4-4f73-434d-92ce-63d2e96c0448" />

## A.Manaual Step

### 0: git clone this repo
```python
git clone https://github.com/xm497/EA_demo__BQ.git
cd EA_demo__BQ
```
### 1: Authenticate with Gcloud command
```python
gcloud auth login
```
This will provide a link by which you can paste the access key
### 2: Configure gcloud to use your project
```python
gcloud config set project <>
```
example :
gcloud config set project xm497-2025-09-16-sqvyh-1


## B.Script Step
run the below script
```python
./00init_setup.sh 
```
### Explanation of the script
- Export it as variable
- Create a VPC
- Create a subnet
- Create a Runtime template
- Create a GCS bucket 
- Upload files to GCS bucket 
- Create Data set and table

## B.Let us verify the setup
 please check the above listed resource are created
- ✅ Go to Colab and runtime template.
  
  <img width="600" height="180" alt="image" src="https://github.com/user-attachments/assets/88174e82-2be7-4c83-a54e-c85028fd8896" />

- ✅ Go to GCS bucket ea-demo-1raw and check the files are placed or not.
  <img width="522" height="390" alt="image" src="https://github.com/user-attachments/assets/4851189d-94ec-4672-929d-77ffb0d9f2b7" />

- ✅ Go to BigQuery and see the Dataset/tables are created.
  
  <img width="300" height="250" alt="image" src="https://github.com/user-attachments/assets/ff68ab73-c1ad-4ee3-bab4-d5c2031c0a62" />

 

