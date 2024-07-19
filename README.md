## Anaplan API Template
Code to get up and running and build solutions using the Anaplan API
Includes integration 
* Python Pandas 
* GCloud functions
* GCloud Storage


## Install
1. Copy to a new folder or download repository
2. from the config directory copy .env file
3. Check Login Credentials in .env file
4. Set workspaceId and modelId in .env file
5. Install requirements.txt

```commandline
copy config/.env .
pip install -r requirements.txt
```

## Functions
### get id's of integration items

* get_import_id
* get_file_id
* get_export_id
* get_delete_action_id
* get_workspace_id
* get_model_id


### download upload functions
* download_export_as_df
* upload_df_and_run_import


## gcloud Functions

gcloud auth login
gcloud functions logs read --limit=50

## gsutil functions
```commandline
gsutil ls
```
