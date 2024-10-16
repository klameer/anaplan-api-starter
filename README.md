## Anaplan API Template
Code to get up and running and build solutions using the Anaplan API
Includes integration 
* Python Pandas 
* GCloud functions
* GCloud Storage


## Install
Installation can be done either by cloning the Git repository or downloading it from:

https://github.com/klameer/anaplan-api-starter

1. Copy to a new folder or download repository
```commandline
git clone https://github.com/klameer/anaplan-api-starter.git
```
2. Create a Python Virtualenv
```commandline
virtualenv venv
venv\scripts\activate
```

3. Install Python libraries
```commandline
pip install -r requirements.txt
```

2. from the config directory copy .env file
```commandline
copy config\.env .
```

3. Update Login Credentials in copied .env file
4. Set workspaceId and modelId in .env file


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

