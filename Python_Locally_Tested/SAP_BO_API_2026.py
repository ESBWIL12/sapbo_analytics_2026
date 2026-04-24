import requests
import urllib3
import pandas as pd
import logging
import os
import time
import json
from datetime import datetime


class webi_Document: #WEBI Document Object to hold the structured details for the MetaData extraction
    def __init__(self, document_id: str, document_cuid: str, document_name: str, folder_id: str, full_path: str, updated: str, scheduled: str, created: str, lastAuthor: str):
        self.document_id = document_id
        self.document_cuid = document_cuid
        self.document_name = document_name
        self.folder_id = folder_id
        self.full_path = full_path
        self.updated = updated
        self.scheduled = scheduled
        self.created = created
        self.lastAuthor = lastAuthor
        self.data_providers: list = [] # Container for data providers details
        self.Execution_Time_Sec: float = 0.0
        self.Number_of_Data_Providers: int = 0
        self.Number_of_API_Calls: int = 0
        self.API_Pause_Counts: int = 0
        self.Start_Time: datetime = None
        self.End_Time: datetime = None
        self.Extraction_Stats: str = None
    
    def Change_extraction_status(self, status: str): # Change the extraction status of the WEBI document object
        self.Extraction_Stats = status

    def add_data_provider(self, data_provider: dict): # Add details to the WEBI document data provider container, expect details to be in a dictionary format
        if isinstance(data_provider, dict):
            self.data_providers.append(data_provider)
            logging.info(f"Added data provider ID {data_provider.get('Data_Provider_ID')} to WEBI Document ID {self.document_id}.")
        else:
            logging.warning("Data provider should be a dictionary.")
    
    def get_details(self) -> dict: # Get the WEBI document details as a dictionary
        return {
            "Document_Id": self.document_id,
            "Document_CUID": self.document_cuid,
            "Folder_Id": self.folder_id,
            "Full_path": self.full_path,
            "Document_name": self.document_name,
            "updated": self.updated,
            "scheduled": self.scheduled,
            "created": self.created,
            "lastAuthor": self.lastAuthor
        }
    
    def add_Extraction_Output(self, input: dict): # Add the extraction execution details to the WEBI document object
        self.Execution_Time_Sec: float = input.get("Execution_Time_Sec", 0.0)
        self.Number_of_Data_Providers: int = input.get("Number_of_Data_Providers", 0)
        self.Number_of_API_Calls: int = input.get("Number_of_API_Calls", 0)
        self.API_Pause_Counts: int = input.get("API_Pause_Counts", 0)
        self.Start_Time: datetime = input.get("Start_Time", None)
        self.End_Time: datetime = input.get("End_Time", None)
    
    def save_webi_json(self, folder_path: str): # Save the WEBI document details and data providers to a JSON file
        folder_path_J = folder_path + f"\\WEBI_Documents_JSON"
        if not os.path.exists(folder_path_J):
            os.makedirs(folder_path_J)
            logging.info(f"Created directory for WEBI Document JSON: {folder_path_J}")
        # Check if file already exists, if so, append a number to the filename
        file_Name=f"{folder_path_J}\\WEBI_Document_{self.document_id}.json"
        #### Skipping duplicate file name check for JSON all new versions will overwrite existing file
        # if os.path.exists(file_Name):
        #     count = 1
        #     while True:
        #         new_file_Name = f"{folder_path}\\WEBI_Document_{self.document_id}({count}).json"
        #         if not os.path.exists(new_file_Name):
        #             file_Name = new_file_Name
        #             break
        #         count += 1
        try:
            folder_path_X = folder_path + f"\\WEBI_Documents_Excel"
            if self.save_webi_excel(folder_path_X):
                logging.info(f"Excel saved for {self.document_id}")
        except Exception as e:
            logging.error(f"Saving Excel for docuemnt ID {self.document_id} with error{e}")
        with open(file_Name, "w", encoding="utf-8") as json_file:
            json.dump({
                "Document_Details": self.get_details(),
                "Extraction_Stats": {
                    "Execution_Time_in_seconds": self.Execution_Time_Sec,
                    "Extraction_Stats": self.Extraction_Stats,
                    "Number_of_Data_Providers": self.Number_of_Data_Providers,
                    "Number_of_API_Calls": self.Number_of_API_Calls,
                    "API_Pause_Counts": self.API_Pause_Counts,
                    "Start_Time": str(self.Start_Time),
                    "End_Time": str(self.End_Time)
                },
                "Data_Providers": self.data_providers
            }, json_file, ensure_ascii=False, indent=4)

    def save_webi_excel(self, folder_path: str) -> bool:
        Saved_Status: bool =False
        chunk_size = 32000
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logging.info(f"Created directory for WEBI Document JSON: {folder_path}")
            
        file_name=f"{folder_path}\\{self.document_id}_WEBI_MetaData.xlsx"
        
        #### Skipping duplicate file name check for EXCEL all new versions will overwrite existing file
        # if os.path.exists(file_name):
        #     count = 1
        #     while True:
        #         new_file_Name = f"{folder_path}\\{self.document_id}_WEBI_MetaData({count}).xlsx"
        #         if not os.path.exists(new_file_Name):
        #             file_name = new_file_Name
        #             break
        #         count += 1
        DataProvider_to_Excel: list =[]
        for DP_SQL in self.data_providers:
            connectionDetails=DP_SQL.get("Connection",{})
            indivisual_SQL:str = ""
            is_partial: bool= False
            if isinstance(DP_SQL, dict):
                indivisual_SQL = DP_SQL.get("SQL_Query", None)
            if len(indivisual_SQL)> chunk_size :
                is_partial=True
                SQL_chunks = [indivisual_SQL[i:i+chunk_size] for i in range(0, len(indivisual_SQL), chunk_size)]
                logging.info(f"SQL too long, chopped into {len(SQL_chunks)} blocks")
            else:
                SQL_chunks =[indivisual_SQL] ### more implementations
                
            for chunk in SQL_chunks:
                if len(SQL_chunks)>1:
                    sql_index=f"{DP_SQL.get("SQL_Index")}_{SQL_chunks.index(chunk)}"
                else:
                    sql_index=f"{DP_SQL.get("SQL_Index")}"

                SQL_record:dict ={
                    "source_reporting_system": "SAP_Business_Object",
                    "source": self.document_name,
                    "raw": "",
                    "dataprovider_id":DP_SQL.get("Data_Provider_ID"),
                    "datasource_Id": DP_SQL.get("DataSource_ID"),
                    "name": DP_SQL.get("Data_Provider_Name"),
                    "datasource_type":DP_SQL.get("DataSource_Type"),
                    "datasource_Cuid": DP_SQL.get("DataSource_CUID"),
                    "data_refresh_time": DP_SQL.get("Data_Profider_Refresh_Time"),
                    "is_Partial": is_partial,
                    "caption": DP_SQL.get("DataSource_Name"),
                    "directory": DP_SQL.get("DataSource_Name"),
                    "connection_name": connectionDetails.get("name"),
                    "sql_index": sql_index,
                    "sql_query": chunk,
                    "row_Count": "",
                    "document_id": self.document_id,
                    "document_cuid": self.document_cuid,
                    "connection_type": connectionDetails.get("@type"),
                    "authentication": "",
                    "class": connectionDetails.get("@type"),
                    "dbname": connectionDetails.get("database"),
                    "folder_path": self.full_path,
                    "report_owner": self.created,
                    "report_creator": self.created,
                    "last_run_date": self.updated,
                    "max_varchar_size": "",
                    "odbc_connect_string_extras": "",
                    "one_time_sql": "",
                    "schema": "",
                    "server": "",
                    "server_oauth": "",
                    "server_userid": "",
                    "service": "",
                    "username": "",
                    "warehouse": "",
                    "workgroup_auth_mode": ""
                }
                DataProvider_to_Excel.append(SQL_record)
        DataProvider_to_Excel_df=pd.DataFrame(DataProvider_to_Excel) 
        # print(DataProvider_to_Excel_df.dtypes)    
        with pd.ExcelWriter(file_name) as writer:
            # Dataframe_Long.to_excel( writer, sheet_name='Json_01', index=False)
            DataProvider_to_Excel_df.to_excel(writer, sheet_name='datasources', index=False)

        return Saved_Status

        
def main():
    # Disable SSL warnings (only for testing)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    Todyay_Date = datetime.now().strftime("%Y-%m-%d")
    # logging.basicConfig(
    #     filename=f"Script_Prod_{Todyay_Date}.log",          # Dev Test Log file name
    #     # filename='Script_Prod_MB.log',          #Prod Log file name
    #     level=logging.INFO,          # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
    #     format='%(asctime)s - %(levelname)s - %(message)s'
    # )


    print(f"Script started.")
    # bo_url = "https://wavgbcdfp1088.atradiusnet.com:8443/biprws"

    ## Development SAP BO REST Server URL 
    bo_url = "https://bobidev.atradiusnet.com:8443/biprws"
    # ------------------------Block to switch between Dev and Prod settings---------------------------

    username = "ESBWIL1"
    password = "Built2@escape"
    auth_type = "secLDAP"    
    DOC_Output = []
    Output = []
    API_CALLS = {"Total Calls":0, "Document Calls":0, "Pauses":0}
    log_status: bool = False
    
    # logging.info(f"Loading list of reports from {Report_list_filename}")
    # Load the source Excel file
    # try:
    #     webi_list_df = pd.read_csv(Report_list_filename, dtype=str)
    #     Connection_df = pd.read_excel(connection_dictionaryFile, dtype=str)
    #     FolderPath_New = FolderPath + f"\\Extraction_{Todyay_Date}"
    #     if not os.path.exists(FolderPath_New):
    #         os.makedirs(FolderPath_New)
    #         logging.info(f"Created directory for API responses: {FolderPath_New}")
    #     FolderPath=FolderPath_New
    # except Exception as e:
    #     logging.error(f"Error initialize file and folder access: {e}")
    #     return


    # Step 1: Log in and get token
    logon_url = f"{bo_url}/logon/long"
    logon_payload = {
        "userName": username,
        "password": password,
        "auth": auth_type
    }
    headers = {"Content-Type": "application/json"}
    print(f"API URL Used for Logon: {logon_url}s")
    try:
        response = requests.post(logon_url, json=logon_payload, headers=headers, verify=False)
        API_CALLS["Total Calls"] += 1
        logon_token = response.headers.get("X-SAP-LogonToken", None)
        print(f"Logon Token:{logon_token}")
        log_status = True if logon_token else False
    except Exception as e:
        print(f"Failed to logon SAP BO Server: {logon_url} with error: {e}, exiting script.")
        log_status = False
        return

    if log_status is True:
        print("Successfully logged in to BO REST API.")

        # try:
        #     for index, row in webi_list_df.iterrows():
        #         Starting_Time=datetime.now()
        #         End_time=datetime.now()
        #         document=row['Report ID']
        #         WEBI_Doc_Status: str=str(row['Extracted Status']).strip()
        #         if WEBI_Doc_Status is not None:
        #             if WEBI_Doc_Status.upper() == "TRUE":
        #                 logging.info(f"Skipping Document ID {document} as Extracted Status is marked True.")
        #                 continue
        #         WEBI_Doc_Status="FALSE"
        #         API_CALLS["Document Calls"] = 0
        #         API_CALLS["Pauses"]=0  # Times the API calls paused due to long response times
        #         number_of_data_providers:int=0
                
        #         # Step 2: API Query for details on Data providers
        #         api_query_url = f"{bo_url}raylight/v1/documents/{str(document)}"
        #         DP_Api_url=f"{bo_url}raylight/v1/documents/{str(document)}/dataproviders"
        #         headers = {
        #                 "Content-Type": "application/json",
        #                 "Accept": "application/json",
        #                 "User-Agent": "Mozilla/5.0",
        #                 "X-SAP-LogonToken": logon_token
        #             }
        #         try:
        #             API_responded = API_Json_CAll(api_query_url, headers, API_CALLS, FolderPath,document)
        #             if isinstance(API_responded, dict) and API_responded.get("error_code") is not None:
        #                 logging.error(f"Response is not for Document ID {document}, error: {API_responded}")
        #                 result = {
        #                     "Document_Id": document,
        #                     "Document_CUID": "Document Not Found on Server",
        #                     "Folder_Id": None,
        #                     "Full_path": None,
        #                     "Document_name": "Document Not Found on Server",
        #                     "updated": None,
        #                     "scheduled": None,
        #                     "created": None,
        #                     "lastAuthor": None
        #                 }
        #                 webi_doc: webi_Document = webi_Document(
        #                     document_id=document,
        #                     document_cuid=None,
        #                     document_name="Document Not Found on Server",
        #                     folder_id=None,
        #                     full_path=None,
        #                     updated=None,
        #                     scheduled=None,
        #                     created=None,
        #                     lastAuthor=None
        #                     )

        #         except Exception as e:
        #             logging.error(f"Error extracting Document Details for Document ID {document}: {e}")
        #             result = {
        #                 "Document_Id": document,
        #                 "Document_CUID": "Document Not Found on Server",
        #                 "Folder_Id": None,
        #                 "Full_path": None,
        #                 "Document_name": "Document Not Found on Server",
        #                 "updated": None,
        #                 "scheduled": None,
        #                 "created": None,
        #                 "lastAuthor": None
        #             }
        #             webi_doc: webi_Document = webi_Document(
        #                 document_id=document,
        #                 document_cuid=None,
        #                 document_name="Document Not Found on Server",
        #                 folder_id=None,
        #                 full_path=None,
        #                 updated=None,
        #                 scheduled=None,
        #                 created=None,
        #                 lastAuthor=None
        #                 )
        #          # Extract document details

        #         if isinstance(API_responded, dict) and API_responded.get("document", {}):
        #             WEBI_Doc_Status = "True"                    
        #             doc = API_responded.get("document", {})
        #             result = {
        #                 "Document_Id": doc.get("id"),
        #                 "Document_CUID": doc.get("cuid"),
        #                 "Folder_Id": doc.get("folderId"),
        #                 "Full_path": doc.get("path"),
        #                 "Document_name": doc.get("name"),
        #                 "updated": doc.get("updated"),
        #                 "scheduled": doc.get("scheduled"),
        #                 "created": doc.get("createdBy"),
        #                 "lastAuthor": doc.get("lastAuthor")
        #             }
        #             #Initialize webi Document Object
        #             webi_doc = webi_Document(
        #                 document_id=doc.get("id"),
        #                 document_cuid=doc.get("cuid"),
        #                 document_name=doc.get("name"),
        #                 folder_id=doc.get("folderId"),
        #                 full_path=doc.get("path"),
        #                 updated=doc.get("updated"),
        #                 scheduled=doc.get("scheduled"),
        #                 created=doc.get("createdBy"),
        #                 lastAuthor=doc.get("lastAuthor")                       
        #             )
        #             webi_doc.Change_extraction_status(WEBI_Doc_Status)

        #             # Step 3: API Query for details on Data providers
        #             try:
        #                 DP_API_responded=API_Json_CAll(DP_Api_url, headers, API_CALLS, FolderPath, document)
        #             except Exception as e:
        #                 logging.error(f"Error extracting list of Data Providers in Document ID {document}: {e}")
        #                 continue

        #             try:
        #                 data_providers = DP_API_responded.get("dataproviders", {}).get("dataprovider", {})
        #                 number_of_data_providers=len(data_providers)
        #                 logging.info(f"Found {len(data_providers)} data providers for document ID {document}")
        #                 for dp in data_providers:
        #                     DP_ID=dp.get("id")
        #                     DataSource_ID=dp.get("dataSourceId")
        #                     DPDetail_Api_url=f"{bo_url}raylight/v1/documents/{str(document)}/dataproviders/{DP_ID}"
        #                     DPDetail_responded=API_Json_CAll(DPDetail_Api_url, headers, API_CALLS, FolderPath, document)
        #                     try:
        #                         DP_Detail = DPDetail_responded.get("dataprovider", {})
        #                         sql_query = ""
        #                         if DP_Detail.get("dataSourceType") == "fhsql":
        #                             properties = DP_Detail.get("properties", {}).get("property",[])
        #                             if isinstance(properties, list):
        #                                 for prop in properties:
        #                                     if prop.get("@key") == "sql":
        #                                         sql_query = prop.get("$", "")
        #                                         Row_index="1"
                                    
        #                             #append individual SQL query to the DP list of details
        #                             Document_Details(Output, doc, dp, DP_Detail, Row_index, sql_query, webi_doc)
        #                         else:
        #                             if DP_Detail.get("dataSourceType") in ["unv","unx"]:
        #                                 Connection_responded:dict = {}
        #                                 if (Connection_df["Universe_ID"] == DataSource_ID).any():   
        #                                     Connection_ID=Connection_df.loc[Connection_df['Universe_ID']==DataSource_ID,'Connection_ID'].values[0]
        #                                     Connection_URL=f"{bo_url}raylight/v1/connections/{Connection_ID}"
        #                                     Connection_responded=API_Json_CAll(Connection_URL, headers, API_CALLS, FolderPath, document)
        #                                 else:
        #                                     logging.warning("Universe not having connection details in .xlsx dictionary file")

        #                                 if isinstance(Connection_responded, dict):
        #                                     DP_Detail["Connection"]=Connection_responded.get("connection",{})
        #                                 else:
        #                                     logging.warning(f"Universe {DataSource_ID} have not found valide Connection information")
        #                                     DP_Detail["Connection"]={
        #                                         "@type": "Relationa",
        #                                         "id": 0,
        #                                         "cuid": "",
        #                                         "name": "",
        #                                         "folderId": 0,
        #                                         "path": "",
        #                                         "database": "",
        #                                         "networkLayer": ""
        #                                     }
        #                                 SQL_query_api_url=f"{bo_url}raylight/v1/documents/{str(document)}/dataproviders/{DP_ID}/queryplan"
        #                                 SQL_query_responded=API_Json_CAll(SQL_query_api_url, headers, API_CALLS, FolderPath, document)
        #                                 try:
        #                                     query_plans = SQL_query_responded.get("queryplan", {})
        #                                     if len(query_plans)!=0:
        #                                         List_Plans=loop_Dict(query_plans)
        #                                         Dataframe = pd.json_normalize(List_Plans)
        #                                         for _, row in Dataframe.iterrows():
        #                                             sql_query = row.get("sql_query", "")
        #                                             Row_index = row.get("index", "")
        #                                             Document_Details(Output, doc, dp, DP_Detail,Row_index, sql_query, webi_doc)
        #                                     else:
        #                                         sql_query="Error retrieving Query Plan"
        #                                         Row_index="0"
        #                                         Document_Details(Output, doc, dp, DP_Detail,Row_index, sql_query, webi_doc)

        #                                 except Exception as e:
        #                                     logging.error(f"Error extracting SQL statement for Data Provider ID {DP_ID} in Document ID {document}: {e}")
        #                                 except ValueError:
        #                                     logging.error(f"Response is not valid JSON for SQL Query of Data Provider ID {DP_ID} in Document ID {document}")
        #                                 if sql_query == "":
        #                                     sql_query = "Data Provider using Universe does not find SQL in Query Plan"
        #                                     Row_index = "0"
        #                                     logging.error(sql_query)
        #                                     Document_Details(Output, doc, dp, DP_Detail, Row_index, sql_query, webi_doc)
        #                             else:
        #                                 sql_query = f"Data Source Type {DP_Detail.get('dataSourceType')} not handled for SQL extraction"
        #                                 #append individual SQL query to the DP list of details
        #                                 Row_index = "0"
        #                                 Document_Details(Output, doc, dp, DP_Detail, Row_index, sql_query, webi_doc)
        #                     except ValueError:
        #                         logging.error(f"Response is not valid JSON for Data Provider Details of Document ID {document}, Data Provider ID {DP_ID}")
        #             except Exception as e:
        #                 logging.error(f"Error extracting SQL statement for Data Provider ID {DP_ID} in Document ID {document}: {e}")
        #             except ValueError:
        #                 logging.error("Response is not valid JSON for Data Provider list of Document ID {document}")
        #         webi_list_df.at[index, 'Extracted Status'] = WEBI_Doc_Status
        #         webi_list_df.at[index, 'Extracted Date'] = Todyay_Date
        #         End_time=datetime.now()
        #         webi_doc.add_Extraction_Output({
        #             "Execution_Time_Sec": (End_time-Starting_Time).total_seconds()-API_CALLS["Pauses"]*5,
        #             "Number_of_Data_Providers": number_of_data_providers,
        #             "Number_of_API_Calls": API_CALLS["Document Calls"],
        #             "API_Pause_Counts": API_CALLS["Pauses"],
        #             "Start_Time": Starting_Time,
        #             "End_Time": End_time
        #         })
        #         result["WEBI_Found_on_Server"]=WEBI_Doc_Status
        #         result["Execution_Time_Sec"]=(End_time-Starting_Time).total_seconds()-API_CALLS["Pauses"]*5
        #         result["Number_of_Data_Providers"]=number_of_data_providers  
        #         result["Number_of_API_Calls"]=API_CALLS["Document Calls"]
        #         result["API_Pause_Counts"]=API_CALLS["Pauses"]
        #         result["Start_Time"]=Starting_Time
        #         result["End_Time"]=End_time
        #         # result["WEBI_Extraction_Status"]=WEBI_Doc_Status
        #         DOC_Output.append(result)
        #         if WEBI_Doc_Status.upper()=="TRUE":
        #             webi_doc.save_webi_json(FolderPath)
        #         logging.info(f"Completed processing Document ID {document} in {result['Execution_Time_Sec']} seconds with {API_CALLS["Document Calls"]} API calls.")
        
        #     log_off_call(bo_url, logon_token, log_status)
        #     API_CALLS["Total Calls"] += 1
        #     Save_Outputs(FolderPath, Output, DOC_Output, webi_list_df, Report_list_filename )
        #     logging.info(f"Total API calls made: {API_CALLS["Total Calls"]} for {webi_list_df['Report ID'].nunique()} WEBI Reports.")
        # except KeyboardInterrupt:
        #     logging.error(f"Keyboard Interrupt detected. Exiting the script.")
        #     log_off_call(bo_url, logon_token, log_status)
        #     API_CALLS["Total Calls"] += 1
        #     Save_Outputs(FolderPath, Output, DOC_Output, webi_list_df, Report_list_filename)
        # except Exception as e:
            # logging.error(f"An error occurred: {e}")
            # log_off_call(bo_url, logon_token, log_status)
            # API_CALLS["Total Calls"] += 1
            # Save_Outputs(FolderPath, Output, DOC_Output, webi_list_df, Report_list_filename)


def Document_Details(Output: list, doc: dict, dp: dict, DP_Detail: dict, Row_index:str, sql_query: str, WEBI_Doc: webi_Document):
    #Put in WEBI Document Object
    Connection=DP_Detail.get("Connection",{})
    Dataprovider:dict = {
        "Data_Provider_ID": dp.get("id"),
        "Data_Provider_Name": dp.get("name"),
        "DataSource_ID":dp.get("dataSourceId"),
        "DataSource_CUID":dp.get("dataSourceCuid"),
        "Data_Profider_Refresh_Time": dp.get("updated"),
        "DataSource_Type": dp.get("dataSourceType"),
        "DataSource_Name": DP_Detail.get("dataSourceName"),
        "SQL_Index": Row_index,
        "SQL_Query": sql_query,
        "Connection_ID":Connection.get("id"),
        "Connection_Name":Connection.get("name"),
        "Connection_Type":Connection.get("@type"),
        "Connection_DataBase":Connection.get("database"),
        "Connection_Network":Connection.get("networkLayer")                            
    }
    WEBI_Doc.add_data_provider(Dataprovider)

    logging.info(f"Appending Data Provider ID {dp.get('id')} details to output.")
    dp_output = {
        "Document_Id": doc.get("id"),
        "Document_CUID": doc.get("cuid"),
        "Folder_Id": doc.get("folderId"),
        "Full_path": doc.get("path"),
        "Document_name": doc.get("name"),
        "updated": doc.get("updated"),
        "scheduled": doc.get("scheduled"),
        "created": doc.get("createdBy"),
        "lastAuthor": doc.get("lastAuthor"),
        "Extraction_Stats": True
    }
    dp_output=dp_output|Dataprovider
    Output.append(dp_output)

def log_off_call(bo_url: str, logon_token: str, log_status: bool):

    if log_status is True: 
        # Step 6: Log off session
        logoff_url = f"{bo_url}/logoff"
        logoff_headers = {"X-SAP-LogonToken": logon_token}
        requests.post(logoff_url, headers=logoff_headers, verify=False)
        log_status: bool = False
        logging.info("Logged off from BO REST API.")

def API_Json_CAll(API_url: str, headers: dict, API_CALLS: dict, FolderPath: str, document) -> dict:
    # logging.info(f"API URL Used for WEBI Document DataProvider Details: {API_url}")
    API_response_TimeFrame=5
    API_Retry_Attempt=3
    API_Call_Sleep_Time=5
    while API_Retry_Attempt >0:
        Before_API_Call=datetime.now()
        logging.info(f"API URL Used for WEBI Document: {API_url}")
        API_responded: dict = {}
        try:
            API_url_CALL = requests.get(API_url, headers=headers, timeout=(3,100), verify=False)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for API URL: {API_url} with error: {e}")
            API_Retry_Attempt -= 1
            API_CALLS["Total Calls"] += 1
            API_CALLS["Document Calls"] += 1
            if API_Retry_Attempt ==0:
                API_responded = API_url_CALL.json()
            continue
            # time.sleep(API_Call_Sleep_Time)
        API_CALLS["Total Calls"] += 1
        API_CALLS["Document Calls"] += 1
        After_API_Call=datetime.now()
        Respond_Time=(After_API_Call - Before_API_Call).total_seconds()
        if Respond_Time> API_response_TimeFrame:
            logging.warning(f"Data Provider list API call for Data Providers took {Respond_Time} seconds. Paulse for {API_response_TimeFrame} seconds.")
            time.sleep(API_Call_Sleep_Time)  # Pause for 5 seconds before proceeding
            API_CALLS["Pauses"] += 1
        if API_url_CALL.status_code == 200:
            API_Retry_Attempt = 0
            try:
                API_responded = API_url_CALL.json()
# #######################################################################################################
                # Saving API response into files stopped. Please resume if need to check. 
                # Save_API_Outputs(FolderPath, API_responded, document, API_url, API_responded)
            except ValueError:
                logging.error(f"Response is not valid JSON for API URL: {API_url}")
            except Exception as e:
                logging.error(f"Error extracting Json: {e}")
        elif API_url_CALL.status_code == 404:
            API_responded = API_url_CALL.json()
            logging.error(f"Error {API_url_CALL.status_code} FOR API Call. Retrying...{API_Retry_Attempt} attempts left.")
            API_Retry_Attempt -= 1
            # time.sleep(API_Call_Sleep_Time)  # Wait before retrying
            # API_CALLS["Pauses"] += 1
        else:
            API_responded = API_url_CALL.json()
            logging.error(f"Error {API_url_CALL.status_code} FOR API Call. with response: {API_responded} ")
            API_Retry_Attempt=0

    return API_responded

def Save_Outputs(FolderPath: str, Output: list, DOC_Output: list, webi_list_df: pd.DataFrame=None, Report_list_filename: str=None):
    webi_list_df.to_csv(Report_list_filename, index=False, quoting=1)
    if Output and DOC_Output:
        try:
            SQLOUTPUT_DF = pd.DataFrame(Output)
            Execution_DF = pd.DataFrame(DOC_Output)
            FileNames={
                "SQL_csv_file":f"{FolderPath}\\WEBI_DataProviders_SQL.csv",
                "Execution__csv_file": f"{FolderPath}\\WEBI_Execution_Stats.csv"
            }
            for key, FileName in FileNames.items():
                if os.path.exists(FileName):
                    count = 1
                    while True:
                        new_file_Name = FileName.replace(".csv", f"({count}).csv")
                        if not os.path.exists(new_file_Name):
                            FileNames[key] = new_file_Name
                            break
                        count += 1 
            SQLOUTPUT_DF.to_csv(FileNames["SQL_csv_file"], index=False, quoting=1)
            Execution_DF.to_csv(FileNames["Execution__csv_file"], index=False, quoting=1)
            logging.info(f"Outputs saved successfully to {FolderPath}\\WEBI_DataProviders_SQL.csv")
        except Exception as e:
            logging.error(f"An error occurred while saving outputs: {e}")   


###########Function built to analyse API response for debugging############
def Save_API_Outputs(FolderPath: str, API_Output: dict, document: str, API_url: str, API_responded: dict):
    FolderPath_1 = FolderPath + f"\\API_Responses"
    if not os.path.exists(FolderPath_1):
        os.makedirs(FolderPath_1)
        logging.info(f"Created directory for API responses: {FolderPath_1}")
    FolderPath_2 = FolderPath_1 + f"\\API_Response_For_{document}"
    if not os.path.exists(FolderPath_2):
        os.makedirs(FolderPath_2)
        logging.info(f"Created directory for API responses: {FolderPath_2}")
    # Save the API response to a JSON file
    api_filename = API_url.split("/")[-1]
    if api_filename == document:
        api_filename = f"{document}_Details"
    else:
        if api_filename == "queryplan":
            DataProvider_ID=API_url.split("/")[-2]
            api_filename = f"{document}_{DataProvider_ID}_{api_filename}_Response"
        else:
            api_filename = f"{document}_{api_filename}_Response"
    file_Name=f"{FolderPath_2}\\{api_filename}.json"
    if os.path.exists(file_Name):
        count = 1
        while True:
            new_file_Name = f"{FolderPath_2}\\{api_filename}({count}).json"
            if not os.path.exists(new_file_Name):
                file_Name = new_file_Name
                break
            count += 1  
    with open(file_Name, "w", encoding="utf-8") as json_file:
        json.dump(API_responded, json_file, ensure_ascii=False, indent=4)


def loop_Dict(Raw_data) -> list: 
    Output: list = []
    index:str=""
    sql_query:str=""
    for key, values in Raw_data.items():
        if isinstance(values, dict):
            Output.extend(loop_Dict(values))
        elif isinstance(values, list):
            Output.extend(loop_List(values))
        else:
            match key:
                case "@index": 
                    index=values
                case "$": 
                    sql_query = values
    if index!="" and sql_query!="":
        Output.append({
            "index": index,
            "sql_query": sql_query
        })

    return Output

def loop_List(Raw_data) -> list: 
    Output: list = []
    index:str=""
    sql_query:str=""
    for prop in Raw_data:
        
        if type(prop) is dict:
            if prop.get("statement",[]):
                Output.extend(loop_List(prop.get("statement",[])))
            elif prop.get("statement",{}):
                    Output.extend(loop_List(prop.get("statement",{})))
            else:
                    Output.extend(loop_List(prop))

            continue
        match prop:
            case "@index": 
                index=Raw_data[prop]
            case "$": 
                sql_query = Raw_data[prop]
    if index!="" and sql_query!="":
        Output.append({
            "index": index,
            "sql_query": sql_query
        })
    return Output

if __name__ == "__main__":
    main()
