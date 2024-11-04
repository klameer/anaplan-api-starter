from functions_anaplan import download_export_as_df

[{'id': '116000000237', 'name': 'EXP: Export Revenue', 'exportType': 'GRID_CURRENT_PAGE', 'exportFormat': 'text/csv', 'encoding': 'UTF-8', 'layout': 'GRID_CURRENT_PAGE'}]

df = download_export_as_df(exportId='116000000237', )
print(df.columns)