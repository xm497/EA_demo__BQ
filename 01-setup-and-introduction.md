## Overview of Setup
The below document will list the overall setup scripts for better understanding of EA Online store

There are mainly 2 steps for the setup, one which run manually and then by script.
To run the manual script, we need to open the dev portal (https://devportal.deutsche-boerse.de/), Then Click on GCP Garage

<img width="845" height="609" alt="image" src="https://github.com/user-attachments/assets/61779bfc-818e-4709-bd00-1b7719544f83" />



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


