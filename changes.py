import pyEDIutils.pasta_api_requests as rq
import pandas as pd
import os

    
def changeroot_to_df(ediroot):
    """
    Convert an Element Tree object to a dataframe. Each element has
    date, packageid, and service method extracted into a row in the 
    dataframe.
    """
    # Iterate over each element in ediroot and extract the variables
    df = pd.DataFrame({'date':[date.text for date in ediroot.iter('date')],
                   'pkgid':[int(ID.text) for ID in ediroot.iter('identifier')],
                   'action':[sm.text for sm in ediroot.iter('serviceMethod')]}
                     )
    return(df)

def drop_duplicates(df):
    """
    PASTA produces some duplicate change records, usually deletes. At last
    count there were 9 duiplicates in the JRN records. This removes those.
    """
    df_dd = df.drop_duplicates()
    n_dupdeletes = df.shape[0] - df_dd.shape[0]
    print('{0} duplicate records were removed.'.format(n_dupdeletes))
    return(df_dd)
    
def archived_changes(archivedir='./edi_requests/', scope='knb-lter-jrn',
                  dedup=True, parsedt=False):
    """
    Load archived PASTA change records from xml files and parse into dataframe.

    Options
        archivedir  path to archive directory string ('./edi_requests')
        scope       EDI scope string ('knb-lter-jrn')
        dedup       remove duplicates boolean (True)
        parsedt     parse 'date' field to datetime index boolean (False) 
    """
    # List files and select scope
    files = os.listdir(archivedir)
    scopefiles = sorted([f for f in files if scope in f])
    # Load each archive, convert to dataframe, and concatenate
    for i, f in enumerate(scopefiles):
        print('Reading archived PASTA request {0}'.format(f))
        root = rq.load_xml(os.path.join('edi_requests', f))
        df = changeroot_to_df(root)
        if i==0:
            df_out = df
        else:
            df_out = pd.concat([df_out, df])
    # dedup and parsedt options
    if dedup:
        df_out = drop_duplicates(df_out)
    if parsedt:
        df_out.index = pd.to_datetime(df_out['date'])
        #, format='%Y-%b-%dT%H:%M:%S.%f')
    return(df_out)


def request_changes(fromdt, todt=None, scope='knb-lter-jrn',
        dedup = True, parsedt=False):
    """
    Request PASTA change records in specified temporal range and parse
    into a dataframe.

    Options
        fromdt      datetime string (required)
        todt        datetime string (None)
        scope       EDI scope string ('knb-lter-jrn')
        dedup       remove duplicates boolean (True)
        parsedt     parse 'date' field to datetime index boolean (False) 
    """
    # An element tree will be returned from the api request
    print('Requesting PASTA changes for {0} from {1} to {2}'.format(
        scope, fromdt, todt))
    root = rq.recent_changes_request(scope, fromdt, todt)
    # Convert elements to rows in dataframe
    df_out = changeroot_to_df(root)
    # dedup and parsedt options
    if dedup:
        df_out = drop_duplicates(df_out)
    if parsedt:
        df_out.index = pd.to_datetime(df_out['date'])
        #, format='%Y-%b-%dT%H:%M:%S.%f')
    return(df_out)

def get_counts(df):
    """
    Add columns that count number of created, updated, and deleted actions
    for each PASTA change. Also add a total package tracker column (1 for 
    creation, -1 for deletion) and a studyid column.
    """
    # Add columns - number of updates and creates, + extracted study id
    df['n_update'] = 0
    df['n_create'] = 0
    df['n_delete'] = 0
    df['n_tot'] = 0
    #convert to str, studyid excludes mistaken 0
    df['studyid'] = df.pkgid.astype(str).str[-6:-3]
    # Fill in number of updates or create for each record
    df.loc[df.action=='updateDataPackage','n_update'] = 1
    df.loc[df.action=='createDataPackage','n_create'] = 1
    df.loc[df.action=='deleteDataPackage','n_delete'] = 1
    # for totals, create = +1, delete = -1
    df.loc[df.action=='createDataPackage','n_tot'] = 1
    df.loc[df.action=='deleteDataPackage','n_tot'] = -1
    return(df)

def counts_to_daily(df, startdt = None):
    """
    Create a daily dataframe with updates, creates, and total packages.
    """
    # Create datetime index first if needed
    df2 = df.copy()
    if not isinstance(df2.index, pd.DatetimeIndex):
        df2.index = pd.to_datetime(df2['date'])
        #, format='%Y-%b-%dT%H:%M:%S.%f')
    if startdt is not None:
        df_out = df2.loc[df2.index > startdt,
                ['n_update', 'n_create', 'n_tot']].resample('D').sum()
    else:
        df_out = df2.loc[:,['n_update','n_create','n_tot']].resample('D').sum()
    return(df_out)

