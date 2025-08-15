import pyodbc
import configparser


def get_sql_credentials():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config["SQL_Credentials"]


def connect_to_sql(database_name):
    credentials = get_sql_credentials()
    conn_str = f'DRIVER={credentials["driver"]};SERVER={credentials["server"]};UID={credentials["sql_uid"]};PWD={credentials["sql_pwd"]};DATABASE={database_name}'
    conn = pyodbc.connect(conn_str)
    return conn


def fetch_medicare_rate_mismatch_data(database_name):
    query = """
    SELECT [ProviderType], [NPI], [LocationTaxId], 
           [Medicare_Allowable_Rate_From_MIRRA], 
           [Medicare_Allowable_Rate_From_SECUR], 
           [Facility_Medicare_Allowable_Rate_From_SECUR]
    FROM [Sharepoint].[MedicareRateMismatchNotification_VW]
    """
    conn = connect_to_sql(database_name)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


def fetch_multiplan_terminated_providers(database_name):
    query = """
            SELECT 
            NPI,
            FirstName,
            MiddleName,
            LastName,
            PrimaryAddress,
            AddressLine1,
            AddressLine2,
            City,
            State,
            ZipCode,
            County,
            Phone,
            TIN,
            TerminationDate,
            ExtractDate,
            ReceivedDate,
            UpdateType,
            Code,
            Description,
            [Is contracted with Secur?],
            [Have any members?]
        FROM [Sharepoint].[MultiplanTerminatedProvidersNotification_VW];
        """
    conn = connect_to_sql(database_name)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows
