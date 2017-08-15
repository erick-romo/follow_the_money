
'''
The Follow The Money ETL extracts data concerning contributions to lawmakers
and candidates at the state level using the Follow The Money API. Additionally
this module inserts the extracted data into a database allowing for front-end
access.
'''

import MySQLdb
import json
import datetime
import time
import re
import requests
from contextlib import contextmanager

import settings

STATES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI',
          'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI',
          'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC',
          'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
          'VT', 'VA', 'WA', 'WV', 'WI', 'WY']


CALLS_THIS_WEEK = 0
CALL_COUNT = 0

class OutOfAPICalls(StandardError):
    def __init__(self, state, page):
        self.state = state
        self.page = page
        StandardError.__init__(self)

    def __repr__(self):
        return "OutOfAPICalls({}, {})".format(self.state, self.page)


DB_CONN = MySQLdb.connect(
	host=settings.host,
	port=3306,
	user=settings.user,
	passwd=settings.passwd,
	db=settings.db
)


@contextmanager
def get_cursor():
	cursor = DB_CONN.cursor()
	yield cursor
	DB_CONN.commit()
	cursor.close()


class Results(object):
    def __init__(self, cursor):
    	self.rows = cursor.fetchall()
    	self.keys = [col[0] for col in cursor.description]
    	self.n = 0

    def get_row(self):
    	if self.n < len(self.rows):
    		row = self.rows[self.n]
    		self.n += 1
    		return row
    	else:
    		return None

    def get_value(self):
        return self.get_row()[0]

    def get_row_dict(self):
    	row = self.get_row()
    	if row is None:
    		return None
    	else:
    		return dict(zip(self.keys, row))

    def __iter__(self):
    	self.n = 0
    	return self

    def next(self):
    	row = self.get_row_dict()
    	if row is None:
    		raise StopIteration()
    	else:
    		return row


def execute_query(query, get_results=False):
	with get_cursor() as cursor:
		cursor.execute(query)
		if get_results:
			return Results(cursor)


def get_page_from_api(state, page):
    '''
    Extract contribution data from the Follow The Money website via API calls.

    Parameters:
        state: (string) the state currently being inserted/updated
        page: (int) the page number to be extracted
    '''
    global CALLS_THIS_WEEK
    global CALL_COUNT
    if CALLS_THIS_WEEK >= settings.api_call_limit:
        raise OutOfAPICalls(state, page)

    url = ("http://api.followthemoney.org/?s={}&f-core=1&c-exi=1&y=2015,2016"
           "&gro=c-t-id&APIKey={}&mode=json&so=c-t-id&p={}").format(
                state, settings.api_key, page)

    data = requests.get(url).json()
    CALLS_THIS_WEEK += 1
    CALL_COUNT += 1
    print "used {} / {} API calls".format(CALLS_THIS_WEEK, settings.api_call_limit)
    return data


def get_page_from_file(state, page):
    """
    Extract contribution data from saved json files.

    Parameters:
        state: (string) the state currently being inserted/updated
        page: (int) the page number to be extracted
    """
    with open("data/{}{}.json".format(state, page)) as f:
        return json.load(f)


def value_for_sql(value):
    """
    Formats the value for an sql insert statement. This means wrapping it in
    quotes if it's a string, and returning "NULL" if it's empty.
    """
    if value == "" or value is None:
        return "NULL"
    elif value.isdigit():
        return value
    else:
        return "'{0}'".format(value.replace("'", "\\'"))


def page_to_inc(page):
    """
    Add a candidate to the incubator table.

    Paramater:
        page: (dict) one page of results from the api
    """
    now = datetime.datetime.now().isoformat()
    rows = []
    for record in page['records']:
        candidate_id = record['Candidate']['id']
        name = record['Candidate']['Candidate']
        state = record['Election_Jurisdiction']['Election_Jurisdiction']
        year = record['Election_Year']['Election_Year']
        cycle_type = record['Election_Type']['Election_Type']

        office = record['Office_Sought']['Office_Sought']
        office_words = office.split()
        if office_words[-1].isdigit():
            district = office_words[-1]
            office = " ".join(office_words[:-1])
        else:
            district = None

        election_status = record['Election_Status']['Election_Status']
        incumbency_status = record['Incumbency_Status']['Incumbency_Status']
        contribution = record['Total_$']['Total_$']
        general_party = record['General_Party']['General_Party']
        specific_party = record['Specific_Party']['Specific_Party']
        row = map(value_for_sql,
                  [candidate_id, name, state, year, cycle_type, office, district,
                  election_status, incumbency_status, general_party,
                  specific_party, contribution, now])
        rows.append("({})".format(", ".join(row)))

    query = """INSERT INTO ftm_inc (Candidate_ID, Full_Name, State, Cycle,
               Cycle_Type, Office, District, Election_Status, Incumbency_Status,
               General_Party, Specific_Party, contribution, timestamp)
               VALUES {};"""
    print query.format(",\n".join(rows))
    execute_query(query.format(",\n".join(rows)))


def update_dim(dim_table, dim_columns, inc_columns=None, order_by_col=None):
    """
    Update a particular dimension table with all new values from the inc table.

    dim_table: (string) Target dimension table
    dim_columns: (list of strings) Columns in the dimension table to add to
    inc_columns: (list of strings) Columns in the incubator to grab. If no list
        is provided, use dim_columns as a default.
    order_by_col: (string) The column to order the inserted rows by in the
        dimension table. If none is given, order by the first dim_column.
    """
    inc_table = "ftm_inc"

    insert = "INSERT IGNORE INTO {} ({})".format(dim_table, ", ".join(dim_columns))

    if inc_columns is None:
        inc_columns = dim_columns
    select = "SELECT DISTINCT {} FROM {}".format(", ".join(inc_columns), inc_table)

    filters = ["{} IS NOT NULL".format(col) for col in inc_columns]
    filters.extend(["{} != ''".format(col) for col in inc_columns])
    where = "WHERE {}".format(" AND ".join(filters))

    if order_by_col is None:
        order_by_col = dim_columns[0]
    order_by = "ORDER BY {} ASC".format(order_by_col)

    sql = " ".join((insert, select, where, order_by))

    print sql
    execute_query(sql)


def update_dims():
    """
    Update dimensions in cand_dim_candidate, ftm_dim_cycle,
    ftm_dim_elect_status, ftm_dim_geo, ftm_dim_office, and ftm_dim_party.
    """
    update_dim("cand_dim_candidate", ["Candidate_ID", "full_name"],
               ["Candidate_ID", "Full_Name"])
    update_dim("ftm_dim_cycle", ["Cycle", "Cycle_Type"])
    update_dim("ftm_dim_elect_status", ["Election_Status"])
    update_dim("ftm_dim_office", ["Office", "Office_Code"])
    update_dim("ftm_dim_party", ["General_Party", "Specific_Party"])

    # update geo dimensions
    sql = ("INSERT IGNORE INTO ftm_dim_geo (State, District) "
           "SELECT DISTINCT State, District FROM ftm_inc;")
    execute_query(sql)


def inc_to_fact():
    """
    Copies inc table into fact table, translating dimensions into their
    appropriate foreign key IDs.
    """
    execute_query("""
        INSERT INTO cand_fact (Candidate_ID, Cycle_ID, Party_ID, Geo_ID,
            Office_ID, Election_Status_ID, contribution)
        SELECT
            Candidate_ID,
            (SELECT Cycle_ID FROM ftm_dim_cycle WHERE ftm_inc.Cycle=ftm_dim_cycle.Cycle and ftm_inc.Cycle_Type=ftm_dim_cycle.Cycle_Type) as Cycle_ID,
            (SELECT Party_ID FROM ftm_dim_party WHERE ftm_inc.General_Party=ftm_dim_party.General_Party and ftm_inc.Specific_Party=ftm_dim_party.Specific_Party) as Party_ID,
            (SELECT Geo_ID FROM ftm_dim_geo WHERE ftm_inc.State=ftm_dim_geo.State and ftm_inc.District=ftm_dim_geo.District) as Geo_ID,
            (SELECT Office_ID FROM ftm_dim_office WHERE ftm_inc.Office=ftm_dim_office.Office) as Office_ID,
            (SELECT Election_Status_ID FROM ftm_dim_elect_status WHERE ftm_inc.Election_Status=ftm_dim_elect_status.Election_Status) as Election_Status_ID,
            contribution;
    """)


def load_pages_into_inc(start_state=STATES[0], start_page=0):
    """
    Cycle through states, grabbing all pages from each state, until we run out
    API calls. The start_state and start_page arguments are used when picking up
    from a previous load.

    start_state: (string) The state to start from.
    start_page: (int) The page to start from.
    """
    state_index = STATES.index(start_state)
    page = start_page
    try:
        max_page = start_page
        for state in STATES[state_index:] + STATES[:state_index]:
            while page <= max_page:
                # get api results
                print "get state {}, page {} from api".format(state, page)
                data = get_page_from_api(state, page)
                with open("data/{}{}.json".format(state, page), 'w') as f:
                    json.dump(data, f)

                # reset maxpage here from the api results
                max_page = data["metaInfo"]["paging"]["maxPage"]
                print "max page for {} is {}".format(state, max_page)

                # write to database
                page_to_inc(data)

                page += 1
            page = 0
        for page in range(start_page):
            print "get state {}, page {} from api".format(start_state, page)
            data = get_page_from_api(start_state, page)
            with open("data/{}{}.json".format(start_state, page), 'w') as f:
                json.dump(data, f)

            page_to_inc(data)

    except OutOfAPICalls as e:
        # We ran out of API calls.
        query = ("INSERT INTO last_page (state, page) "
                 "VALUES ({}, {})").format(e.state, e.page)
        execute_query(query)

    else:
        query = ("INSERT INTO last_page (state, page) "
                 "VALUES ({}, {})").format(STATES[0], 0)
        execute_query(query)


def normalizeOffice(office):
    '''
    The purpose of this funciton is to standarize the office field
    in the incubator and office dimensional tables to the standard
    set in the Lawmaker API Calls

    Parameters:
        office: (string) the name of the office sought by a candidate or inhabited by a lawmaker
    '''
    if 'US HOUSE' in office:
        return 'House of Representatives'
    elif 'HOUSE DISTRICT' in office:
        return "State House/Assembly"
    elif 'ASSEMBLY DISTRICT' in office:
        return 'State House/Assembly'
    elif 'EDUCATION' in office:
        return 'Board of Ed.'
    elif 'SUPREME' in office:
        return 'Supreme Court Seat'
    elif 'APPELLATE' in office:
        return "Apellate Court Seat"
    elif 'SENATE DISTRICT' in office:
        return 'State Senate'
    elif 'State Representative' in office:
        return "State House/Assembly"
    elif 'SENATE' in office and 'US' not in office:
        return "State Senate"
    elif 'GOVERNOR' in office:
        return 'Governor'
    elif 'LIEUTENANT GOVERNOR' in office:
        return 'Lt. Gov'
    elif 'HAWAIIAN AFFAIRS' in office:
        return 'Office of Hawaiian Affairs'
    elif 'PUBLIC REGULATION' in office:
        return 'Public Regulation Comissioner'
    elif "REGENTS" in office:
        return 'Board of Reagents Member'
    elif "SUPERINTENDENT OF PUBLIC" in office:
        return 'Superintendent of Public Instruction'
    elif "TRANSPORTATION COMMISSIONER" in office:
        return "Transportation Commissioner"
    elif "REGIONAL TRANSPORTATION" in office:
        return "Regional Transportation Commissioner"
    elif "SUPERIOR COURT" in office:
        return "Superior Court Seat"
    elif "PUBLIC SERVICE COMMISSIONER" in office:
        return 'Public Service Commissioner'
    else:
        return office.title()

def Office_Code(o):
    '''
    The purpose of this function is to populate the office_code field of the
    incubator and office dimensional tables.

    Note: Where applicable the office codes were taken from those that existed in the er_fact table

    Parameter:
        o: (string) the name of the office sought by a candidate or inhabited by a lawmaker

    '''
    Code=''
    if o=='State House/Assembly':
        Code='SLEG'
    if o=='State Senate':
        Code='SSN'
    if o=='Governor':
        Code='GOV'
    if o=='Lieutenant Governor':
        Code='LTGOV'
    if o=='Board of Ed.':
        Code='BOE'
    if o=='Supreme Court':
        Code='SSC'
    with open ('db.txt') as f:
        content=f.readlines()
    host=content[0][content[0].find("=")+1:].strip()
    user=content[1][content[1].find("=")+1:].strip()
    passwd=content[2][content[2].find("=")+1:].strip()
    db=content[3][content[3].find("=")+1:].strip()
    conn = MySQLdb.connect(host,user,passwd,db)
    cursor = conn.cursor()
    OFF="""UPDATE ftm_dim_office SET Office_Code='%s' WHERE Office='%s'"""
    OFF2="""UPDATE ftm_inc SET Office_Code='%s' WHERE Office='%s'"""
    #print OFF%(Code,o)
    cursor.execute(OFF%(Code,o))
    cursor.execute(OFF2%(Code,o))
    conn.commit()
    conn.close()


def get_api_call_count():
    """
    Get the number of calls so far this week, and set the CALLS_THIS_WEEK variable.
    """
    global CALLS_THIS_WEEK
    year, week, day = datetime.datetime.now().isocalendar()
    sql = "SELECT SUM(calls) FROM api_calls WHERE year={} and week = {}".format(
        year, week)
    print sql
    calls = execute_query(sql, get_results=True).get_value()
    print "result = {}".format(calls)
    CALLS_THIS_WEEK = 0 if calls is None else int(calls)


def save_api_call_count():
    """
    Save the number of calls we've just made.
    """
    year, week, day = datetime.datetime.now().isocalendar()
    sql = """INSERT INTO api_calls (year, week, calls)
             VALUES ({}, {}, {});""".format(year, week, CALL_COUNT)
    print sql
    execute_query(sql)


def get_start_page():
    """
    Get the page to start with from the ftm_update database.
    """
    sql = "SELECT state, page FROM last_page ORDER BY id DESC LIMIT 1"
    state, page = execute_query(sql, get_results=True).get_row()
    return state, page


if __name__ == "__main__":
    get_api_call_count()
    print "api calls so far: {} / {}".format(CALLS_THIS_WEEK, settings.api_call_limit)
    start_state, start_page = get_start_page()
    try:
        load_pages_into_inc(start_state=start_state, start_page=start_page)
    finally:
        save_api_call_count()
