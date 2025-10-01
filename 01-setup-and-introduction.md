## Overview of Setup
The below document will list the overall setup scripts for better understanding of EA Online store

There are mainly 2 steps for the setup, one which run manually and then by script.

## A.Manaual Step

### 0: git clone this repo
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


