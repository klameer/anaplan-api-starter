import functions_framework
import pandas as pd
from functions_general import download_csv_to_df
from functions_anaplan import upload_df_as_single_chunk
from functions_general import start_clock, stop_clock
from functions_anaplan import run_process
from functions_anaplan import download_export_as_df
from functions_general import upload_df_to_GCS
from functions_general import download_csv_to_content
from functions_anaplan import start_export_till_end
import io
import traceback

## Download Files

tempdir = 'downloads/'

files = {
    '116000000000': 'EXP Triskell Ext Resources CRO FTE Categories',
    '116000000001': 'EXP Triskell Ext Resources CRO FTE Values',
    '116000000002': 'EXP Triskell Export Phase by Project by Month',
    '116000000004': 'EXP Triskell Ext Resources FfS Other Categories',
    '116000000005': 'EXP Triskell Ext Resources FfS Other Values',
    '116000000006': 'EXP Triskell Int Resources Categories',
    '116000000007': 'EXP Triskell Int Resources Values',
    '116000000008': 'EXP Triskell Int Resources Base Currency'
}


def download_files(local=False):
    for key, value in files.items():
        print(f'starting export: {key}')
        status = start_export_till_end(key)
        print(status)
        print(f'downloading: {value}')
        df = download_export_as_df(key)
        if local:
            df.to_csv(tempdir + value + '.csv', index_label='Key')
        else:
            upload_df_to_GCS(df, 'triskell-integration', value + '.csv', key=False)

## Create merged Files



def make_final(cat, val, filename, local=False):
    if local:
        df_phase = pd.read_csv(tempdir + 'EXP Triskell Export Phase by Project by Month.csv')
    else:
        content = download_csv_to_content('triskell-integration', 'EXP Triskell Export Phase by Project by Month.csv')
        df_phase = pd.read_csv(io.StringIO(content))

    if local:
        df_categories = pd.read_csv(tempdir + cat)
    else:
        content = download_csv_to_content('triskell-integration', cat)
        df_categories = pd.read_csv(io.StringIO(content))

    if local:
        df_values = pd.read_csv(tempdir + val)
    else:
        content = download_csv_to_content('triskell-integration', val)
        df_values = pd.read_csv(io.StringIO(content))

    df_final = pd.merge(df_categories, df_values, on=['Versions', 'Projects P4 #', 'Project Budget Line Items'])
    df_final = pd.merge(df_final, df_phase, on=['Versions', 'Projects P4 #', 'Time'])

    if local:
        df_final.to_csv(tempdir + filename, index_label='Key')
    else:
        upload_df_to_GCS(df_final, 'triskell-integration', filename, key=True)


def create_merged_files(local=False):
    filename = 'FINAL Ext Resources CRO FTE.csv'
    print(f'make {filename}')
    make_final('EXP Triskell Ext Resources CRO FTE Categories.csv',
               'EXP Triskell Ext Resources CRO FTE Values.csv',
               filename,
               local=local)

    filename = 'FINAL Ext Resources FfS Other.csv'
    print(f'make {filename}')
    make_final('EXP Triskell Ext Resources FfS Other Categories.csv',
               'EXP Triskell Ext Resources FfS Other Values.csv',
               filename,
               local=local)

    filename = 'FINAL Int Resources.csv'
    print(f'make {filename}')
    make_final('EXP Triskell Int Resources Categories.csv',
               'EXP Triskell Int Resources Values.csv',
               filename,
               local=local)

    filename = 'FINAL Int Resources Base Currency.csv'
    print(f'make {filename}')
    make_final('EXP Triskell Int Resources Categories.csv',
               'EXP Triskell Int Resources Base Currency.csv',
               filename,
               local=local)

def upload_to_anaplan():
    files = {
        '113000000082': 'FINAL Ext Resources CRO FTE.csv',
        '113000000083': 'FINAL Ext Resources FfS Other.csv',
        '113000000085': 'FINAL Int Resources.csv',
        '113000000084': 'FINAL Int Resources Base Currency.csv'
    }

    for key, value in files.items():
        df = download_csv_to_df('triskell-integration', value)
        resp = upload_df_as_single_chunk(key, df)
        print(resp)


def run_all():

    try:
        stop_clock()
        # download_files(local=True)
        # create_merged_files(local=True)

        download_files()
        create_merged_files()

        upload_to_anaplan()
        run_process('118000000013')
        start_clock()
        return 'Files downloaded, transformed and uploaded into Anaplan. Complete!'

    except Exception as e:
        traceback_str = traceback.format_exc()
        start_clock()
        return traceback_str


@functions_framework.http
def run_http(request):
    r_txt = run_all()
    return r_txt


if __name__ == '__main__':
    r = run_all()
    print(r)


