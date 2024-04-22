import requests
import pandas as pd

states = ['ARIZONA', 'COLORADO', 'NEVADA', 'NEW MEXICO', 'CALIFORNIA', 'OREGON', 'WASHINGTON']

base_fields = [
    'CERT', 'STNAME', 'RISDATE', 'NAMEFULL'
]
fields = [
    'CRREAGQ', 'CRRECONQ', 'CRREFORQ', 'CRRELOCQ', 'CRREMULQ', 'CRRENRSQ', 'CRRENUSQ',
    'CRREOFFDOMQ', 'CRREQ', 'CRRERESQ', 'CRRERS2Q', 'CRRERSFQ', 'ELNATQ', 'IDNCCIR',
    'IDNTCIQR', 'IDNTCIR', 'LNOT1T3', 'LNOT3LES','LNOT3T12', 'LNOT3T5', 'LNOT5T15',
    'LNOTCI', 'LNOTOV15', 'LNPLEDGER', 'LNRE', 'LNRECNOT', 'LNRECONS', 'LNREDOM',
    'LNREMULT', 'LNRENR4N', 'LNRENRES', 'LNRENROT', 'LNRENROW', 'LNRERES', 'LNRESNCR',
    'LNRS1T3', 'LNRS3LES', 'LNRS3T12', 'LNRS3T5', 'LNRS5T15', 'LOREGTY', 'LOTH', 'LRECONS', 
    'LREMULT', 'LSALNLS', 'LSAORE', 'LSASCDBT', 'MSRESFCL', 'NACI', 'NAGTY', 'NAGTYGNM', 
    'NAGTYPAR', 'NALCI', 'NALGTY', 'NALNSALE', 'NALOTH', 'NALREAG', 'NALRECON', 'NALREMUL', 
    'NALREMULR', 'NALTOT', 'NARE', 'NARECONS', 'NARELNDO', 'NAREMULT', 'NARENRES', 'NARENROT',
    'NARSLNLT', 'NASSETR', 'NETGNAST', 'NETGNSLN', 'NTCI', 'NTCIQ', 'NTLNLS', 'NTRE', 'NTRECONS', 
    'NTREMULQ', 'NTREMULT'
]

start_year = 2023
end_year = 2024
start_qtr = 1
end_qtr = 1

yearQtrs = {
    'Q1': '0331',
    'Q2': '0630',
    'Q3': '0930',
    'Q4': '1231'
}




def get_risdate(year, qnum):
    year = str(year)
    risdate = year + yearQtrs['Q' + str(qnum)]
    return risdate



def get_range_data(fields, start_year, start_qtr, end_year, end_qtr):
    cur_year = start_year
    cur_qtr = start_qtr
    df = pd.DataFrame()
    get_more = True

    while get_more == True:
        print(str(cur_year) + 'Q' + str(cur_qtr))
        temp_data = get_data(fields, cur_year, cur_qtr)
        df = pd.concat([df, temp_data])

        if (cur_year == end_year and cur_qtr == end_qtr):
            get_more = False

        if cur_qtr == 4: 
            cur_year += 1
            cur_qtr = 1
        else:
            cur_qtr += 1
        print('End: ' + str(cur_year) + 'Q' + str(cur_qtr))
        

    field_labels = get_labels(fields)

    df = df.rename(columns=field_labels)

    df.to_excel("BankInfo.xlsx")


def get_data(fields, year, qtr):
    offset = 0
    limit = 1000

    state_filter = ''

    dep_filter = "DEP:[500000 TO *] AND "

    yearQtrStr = str(year) + 'Q' + str(qtr)

    risdate = get_risdate(str(year), qtr)

    query_fields = base_fields + fields
    
    if len(states) > 0:
        state_filter = ' AND STNAME: ("' + '", "'.join(states) + '")'

    response = requests.get(f"https://banks.data.fdic.gov/api/financials?limit={limit}&fields={','.join(query_fields)}&filters={dep_filter}RISDATE:{risdate}{state_filter}")

    rjson = response.json()

    total_records = rjson['meta']['total']

    df = pd.json_normalize(rjson['data'])
    
    return df







def get_labels(fields):

    field_query = "VARIABLE:(" + ' OR '.join(fields) + ")"

    response = requests.get(f"https://pfabankapi.app.cloud.gov/api/definitions?limit=2000&filters={field_query}")

    rjson = response.json()['data']

    field_labels = {}

    for f in rjson:
        field_labels['data.' + f['data']['VARIABLE']] = f['data']['LONG_DESCRIPTION'] + ' ' + f['data']['UNIT_TYPE'] + ' ' + f['data']['INCOME_BASIS']

    return field_labels