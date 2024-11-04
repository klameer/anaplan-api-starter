from functions_anaplan import \
    list_export_files, \
    list_exports, \
    list_user_workspaces, \
    list_imports, \
    list_files, \
    list_delete_actions, \
    list_processes, \
    list_models, \
    get_model_id, \
    get_file_id, \
    get_export_id, \
    get_workspace_id, \
    get_import_id, \
    get_delete_action_id, \
    get_process_id

print('list_models(), get_model_id()'.ljust(50, '*'))
l = list_models()
print(get_model_id(l[0]['name']))

print('list_exports(), get_export_id()'.ljust(50, '*'))
l = list_exports()
print(get_export_id(l[0]['name']))


print('list_export_files(), get_file_id()'.ljust(50, '*'))
l = list_export_files()
print(get_file_id(l[0]['name']))


print('list_user_workspaces(), get_workspace_id()'.ljust(50, '*'))
l = list_user_workspaces()
print(get_workspace_id(l[0]['name']))

print('list_imports(), get_import_id()'.ljust(50, '*'))
l = list_imports()
print(get_import_id(l[0]['name']))

print('list_files(), get_file_id()'.ljust(50, '*'))
l = list_files()
print(get_file_id(l[0]['name']))

print('list_delete_actions(), get_delete_action_id()'.ljust(50, '*'))
l = list_delete_actions()
print(get_delete_action_id(l[0]['name']))

print('list_processes(), get_process_id()'.ljust(50, '*'))
l = list_processes()
print(get_process_id(l[0]['name']))
