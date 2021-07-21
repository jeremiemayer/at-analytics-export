# coding : utf-8
import sys
import json
import yaml #pyyaml
import http.client
import calendar
import pyodbc
import concurrent
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import URL
from sqlalchemy.sql import insert
from math import ceil
from copy import deepcopy, copy
from datetime import datetime, timedelta, date

get_data = "/v3/data/getData"
get_row_count = "/v3/data/getRowCount"

config = yaml.safe_load(open('config.yaml'))
sql_config = config['sql']

# setup dates
# use yesterdays date since we cannot retrieve data from current day
now = datetime.now() - timedelta(days=1) 
day = now.day
yr = now.year
month = now.month

last_day = calendar.monthrange(yr,month)[1]
year_start = yr if month>=10 else yr-1
season = yr+1 if month>=10 else yr

daily_range = {
    'type':'D',
    'start':'{}-{}-{}'.format(yr,str(month).zfill(2),str(day).zfill(2),),
    'end':'{}-{}-{}'.format(yr,str(month).zfill(2),str(day).zfill(2),),
}

monthly_range = {
    'type':'D',
    'start':'{}-{}-01'.format(yr,str(month).zfill(2),str(day).zfill(2),),
    'end':'{}-{}-{}'.format(yr,str(month).zfill(2),str(last_day).zfill(2),),
}

yearly_range = {
    'type':'D',
    'start':'{}-{}-{}'.format(year_start,10,'01'),
    'end':'{}-{}-{}'.format(yr,str(month).zfill(2),str(day).zfill(2)),
}

def createDBConnection(config):
    """
    Connect to database

    :param config: Dictionary containing the database parameters
    :return: SQLAlchemy engine
    """
    connection_string="DRIVER={};SERVER={};DATABASE={};UID={};PWD={};PORT={}".format(
        config['os_driver'],
        config['server'],
        config['db'],
        config['user'],
        config['password'],
        config['port']
    )
    connection_url = URL.create(config['driver'],query={"odbc_connect":connection_string})

    engine = create_engine(connection_url)

    return engine

headers = {
    'x-api-key': config['keys']['api']+'_'+config['keys']['secret'],
    'Content-type': "application/json"
}

def buildPayload(params):

    payloads = []

    base_payload = {
        "columns": params.cols.split(','),
        "space": {
            "s": list(map(int,params.site_id.split(','))) # requires list of integers
        },
        "period": {
            "p1": []
        }
    }
    
    # daily_range would just be the records for "today", no need to delete all records everytime
    if params.run_monthly:
        monthly_payload = deepcopy(base_payload)
        monthly_payload['period']['p1'] = [monthly_range] if not params.run_daily else [daily_range]
        payloads.append({'name':calendar.month_name[month],'payload':monthly_payload})

    if params.run_yearly:
        yearly_payload = deepcopy(base_payload)
        yearly_payload['period']['p1'] = [yearly_range] if not params.run_daily else [daily_range]
        payloads.append({'name':'All','payload':yearly_payload})
    
    return payloads

def getRequests():
    """
    Get all requests from SQL and build the "payload" (json request) for each.
    Each payload is sent to server and data is returned
    """

    sql = """
        select	r.request_id, r.import_schema, r.import_table, sites.sites site_id,
                r.run_daily, r.run_monthly, r.run_yearly, r.run_seasonally, 
                cols.cols
        from	ata.requests r
        join	(
            select	request_id, string_agg(c.ata_code,',') cols
            from	ata.request_columns rc
            join	ata.columns c
                on	c.column_id=rc.column_id
            group by request_id
        ) cols
            on	cols.request_id=r.request_id
        join	(
            select	request_id, string_agg(c.ata_code,',') sites
            from	ata.request_sites rc
            join	ata.sites c
                on	c.site_id=rc.site_id
            group by request_id
        ) sites
            on	sites.request_id=r.request_id
        where   r.is_active=1
    """

    # connect
    with engine.connect() as conn:

        rs = conn.execute(sql)
        for row in rs:
            payloads = buildPayload(row)

            for payload in payloads:
                getData(payload,row)

def makeRequest(url,payload,page=0):
    """
    Get result of single request

    :param url: AT analytics URL to either get the data or the row count for the request
    :param payload: Dictionary containing the request parameters
    :param page: When results are >10000 rows, request needs to paginate, page identifies which set of records should be returned
    :return: Dict of results, in JSON format
    """
    curr_payload = deepcopy(payload)
    if page>0:
        curr_payload['page-num'] = page

    try:
        conn = http.client.HTTPSConnection("api.atinternet.io")
        conn.request("POST",url,json.dumps(curr_payload),headers)
        res = conn.getresponse()
        data = res.read()
        conn.close()

    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

    try:
        js = json.loads(data)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        print(data)
        raise

    return js

def export(data,name,params):
    """
    Export values to SQL

    :param data: Dictionary containing results from AT
    :param name: Name of current set of data (Day,Month,Year,Season)
    :param params: SQL row containing table name, column names, etc..
    """
    connection_string="DRIVER={};SERVER={};DATABASE={};UID={};PWD={};PORT={}".format(
        sql_config['os_driver'],
        sql_config['server'],
        sql_config['db'],
        sql_config['user'],
        sql_config['password'],
        sql_config['port']
    )
    
    # check that list is not empty
    if data:
        conn = pyodbc.connect(connection_string,autocommit=True)
        crsr = conn.cursor()

        # delete same set of records that are being imported, as to not have duplicates
        if params.run_daily:
            delete = "delete from {}.{} where cut='{}' and season={} and date=dateadd(day,-1,convert(date,getdate()))".format(params.import_schema,params.import_table,name,season)
        else:
            delete = "delete from {}.{} where cut='{}' and season={}".format(params.import_schema,params.import_table,name,season)

        crsr.execute(delete)

        crsr.fast_executemany = True

        # create column list based on first row of data - the order changes when its returned by AT so we can't use our own list
        cols = data[0].keys()

        # create parametrized string
        sql_params = ','.join(['?' for s in cols])+',?,?'

        # cut,season will be default columns that should exist in all tables
        insert = 'insert into {}.{} ({},cut,season) values ({})'.format(params.import_schema,params.import_table,','.join(cols),sql_params)
        
        # convert list of dicts to list of lists (pyodbc doesn't accept dictionaries)
        lst = []

        for row in data:
            cur_list = list(row.values())+[name,season]            
            lst.append(cur_list)
        
        crsr.executemany(insert,lst)

def getData(payload,params):
    """
    Get results of request made to AT Analytics platform. Paginate through and merge results if required.

    :param payload: Dictionary containing the request parameters
    :return: List of results from AT Analytics
    """

    # setup payload to request the total number of rows so we can paginate if needed
    # remove properties which aren't required (these would cause the request to fail)
    payload_data = payload['payload']
    payload_name = payload['name']

    row_count_payload = copy(payload_data)
    entries_to_remove = ('sort','max-results','page-num')
    for k in entries_to_remove:
        row_count_payload.pop(k,None)

    rows = makeRequest(get_row_count,row_count_payload)
    row_count = rows.get('RowCounts')[0].get('RowCount')

    # at-internet analytics max rows is 10K
    rows_per_page = 10000
    total_pages = ceil(row_count/rows_per_page)

    payload_data['max-results'] = rows_per_page
    payload_data['sort'] = [payload_data['columns'][0]] # sort is a required field, just use the first column as sorting isn't important for export

    output = []

    # Setup threaded requests, if multiple are needed they can run concurrently (AT-Internet cap is 5)
    pool = ThreadPoolExecutor(max_workers=5)
    futures = [pool.submit(makeRequest,get_data,payload_data,page) for page in range(1,total_pages+1)]
    wait(futures,timeout=None,return_when=ALL_COMPLETED)
    
    for future in concurrent.futures.as_completed(futures):
        try:
            data = future.result()
        except Exception as exc:
            print("Unexpected error:", sys.exc_info()[0])
        finally:
            output += data['DataFeed']['Rows']

    export(output,payload_name,params)
          
engine = createDBConnection(config['sql'])       
getRequests()
