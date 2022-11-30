from os import path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pandas import DataFrame
from enum import Enum
from ez_pandas.ez_pandas import print_df

class ValueInputOption():
    RAW = 'RAW'
    USER_ENTERED = 'USER_ENTERED'

def authenticate() -> Credentials:
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    folder = 'sheets_auth'

    if path.exists(f'{folder}/token.json'):
        creds = Credentials.from_authorized_user_file(f'{folder}/token.json', scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # get credentials from: https://console.cloud.google.com/apis/credentials?project=cdm-tbd
            # control + f5
            # download
            # rename
            # paste to folder
            flow = InstalledAppFlow.from_client_secrets_file(f'{folder}/credentials.json', scopes)
            creds = flow.run_local_server(port=0)

        with open(f'{folder}/token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def df_from_sheet(sheet_values: list) -> DataFrame:
    return DataFrame(sheet_values[1:], columns=sheet_values[0])

def read_sheet(sheet_id: str, sheet_name: str, sheet_range: str = '') -> DataFrame:
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets()

    range = sheet_name if sheet_range == '' else f'{sheet_name}!{sheet_range}'
    result = sheets.values().get(spreadsheetId=sheet_id, range=range).execute()
    values = result.get('values', [])

    if not values:
        print(f'read_sheet: no data found for: id "{sheet_id}" and sheet "{sheet_name}"')
        return None

    df = df_from_sheet(values)
    return df

def df_to_list_of_lists(df: DataFrame) -> list:
    return df.values.tolist()

def create_sheet_tab(sheet_id: str, sheet_name: str):
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
    request = service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body)
    response = request.execute()
    print(response)

def write_sheet(sheet_id: str, sheet_name: str, df: DataFrame, value_input_option: str = ValueInputOption.USER_ENTERED, sheet_range_offset: tuple = ()):
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    values_list = df_to_list_of_lists(df)
    body = {'values': values_list}

    result = 0
    try:
        result = service.spreadsheets().values().update(spreadsheetId=sheet_id, range=sheet_name, valueInputOption=value_input_option, body=body).execute()
    except Exception as e:
        if f'"Unable to parse range: {sheet_name}"' in str(e):
            create_sheet_tab(sheet_id, sheet_name)
            result = service.spreadsheets().values().update(spreadsheetId=sheet_id, range=sheet_name, valueInputOption=value_input_option, body=body).execute()
    finally:
        print(f"write_sheet: {result.get('updatedCells')} cells updated.")
    return result

def do():
    sheet_id = '13Y4e7zbz6yY9KQIFZsdkegMby3ZYBDBX8UP8g107F0g'
    df = read_sheet(sheet_id, 'Sheet2', 'B3:D13')
    write_sheet(sheet_id, 'Sheet4', df, value_input_option=ValueInputOption.RAW)