from functions_anaplan import list_files

def files():
    l = list_files()
    return [f['name'] for f in l['files']]

if __name__ == '__main__':
    print(files())

