from __future__ import print_function
import os.path
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_SPREADSHEET_ID = '13Y4e7zbz6yY9KQIFZsdkegMby3ZYBDBX8UP8g107F0g'
SAMPLE_RANGE_NAME = 'Sheet1!A1:A2'


def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
        #
        # if not values:
        #     print('No data found.')
        #     return
        #
        # print('Name, Major:')
        # for row in values:
        #     # Print columns A and E, which correspond to indices 0 and 4.
        #     print('%s, %s' % (row[0], row[4]))
        print(values)
    except HttpError as err:
        print(err)

# def write():
#     import pygsheets
#     import pandas as pd
#     # authorization
#     gc = pygsheets.authorize('credentials.json')
#
#     # Create empty dataframe
#     df = pd.DataFrame()
#
#     # Create a column
#     df['name'] = ['John', 'Steve', 'Sarah']
#
#     # open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
#     sh = gc.open('PY to Gsheet Test')
#
#     # select the first sheet
#     wks = sh[0]
#
#     # update the first sheet with df, starting at cell B2.
#     wks.set_dataframe(df, (1, 1))

# def write2():
#     import gspread
#     # gc = gspread.oauth()
#     sh = gc.open("Python")
#     print(sh.sheet1.get('A1'))

def update_values(spreadsheet_id, range_name, value_input_option,  _values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
        """
    # creds, _ = google.auth.default()
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # pylint: disable=maybe-no-member
    try:

        service = build('sheets', 'v4', credentials=creds)
        # values = [
        #     [
        #         # Cell values ...
        #     ],
        #     # Additional rows ...
        # ]
        values = _values
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption=value_input_option, body=body).execute()
        print(f"{result.get('updatedCells')} cells updated.")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

def do():
    print('sheets')
    # main()
    # write()
    # write2()
    update_values("13Y4e7zbz6yY9KQIFZsdkegMby3ZYBDBX8UP8g107F0g",
                  "A1:D2", "USER_ENTERED",
                  [
                      ['A', 'B'],
                      ['C', 'D']
                  ])