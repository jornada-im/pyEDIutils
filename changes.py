import pyEDIutils.pasta_api_requests as rq
import pandas as pd
import os

    
def changeroot_to_df(ediroot):
    """
    Convert an Element Tree object to a dataframe. Each element has
    date, packageid, and service method extracted into a row in the 
    dataframe.

    Parameters
    ----------
    ediroot : xml tree
        An XML tree returned from an EDI changes request
    """
    # Iterate over each element in ediroot and extract the variables
    df = pd.DataFrame({'date':[date.text for date in ediroot.iter('date')],
                   'pkgid':[int(ID.text) for ID in ediroot.iter('identifier')],
                   'action':[sm.text for sm in ediroot.iter('serviceMethod')]}
                     )
    return(df)

def drop_duplicates(df):
    """Drop duplicate PASTA database records
    
    PASTA produces some duplicate change records, usually deletes. At last
    count there were 9 duplicates in the JRN records. This removes those.

    Parameters
    ----------
    df : pandas dataframe
        A table of changes from the PASTA database
    """
    df_dd = df.drop_duplicates()
    n_dupdeletes = df.shape[0] - df_dd.shape[0]
    print('{0} duplicate records were removed.'.format(n_dupdeletes))
    return(df_dd)
    
def load_archived_changes(archivedir='./edi_requests/', scope='knb-lter-jrn',
    dedup=True, parsedt=False):
    """
    Load archived PASTA change records from xml files and parse into dataframe.

    Parameters
    ----------
    archivedir : str, optional
        path to archive directory string ('./edi_requests'), by default './edi_requests/'
    scope : str, optional
        EDI scope string, by default 'knb-lter-jrn'
    dedup : bool, optional
        remove duplicates boolean, by default True
    parsedt : bool, optional
        parse 'date' field to datetime index boolean, by default False
    """
    # List files and select scope
    files = os.listdir(archivedir)
    scopefiles = sorted([f for f in files if scope in f])
    # Load each archive, convert to dataframe, and concatenate
    for i, f in enumerate(scopefiles):
        print('Reading archived PASTA request {0}'.format(f))
        root = rq.archived_response_to_ET(os.path.join('edi_requests', f))
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

def archive_requested_changes(fromdt, todt=None, scope='knb-lter-jrn',
    archivedir='./edi_requests/'):
    """
    Request PASTA change records in specified temporal range and parse
    into a dataframe.

    Parameters
    ----------
    fromdt : string
        datetime string (YYYY-MM-DD)
    todt : string, optional
        datetime string (YYYY-MM-DD), by default None
    scope : str, optional
        EDI scope string, by default 'knb-lter-jrn'
    archivedir : str, optional
        path to archive directory string ('./edi_requests'), by default './edi_requests/'
    """
    # An element tree will be returned from the api request
    print('Requesting PASTA changes for {0} from {1} to {2}'.format(
        scope, fromdt, todt))
    response = rq.recent_changes(scope, fromdt, todt)
    # Get outfile
    outfile = os.path.join(archivedir, 
        scope + '_' + fromdt.replace('-', '') + '-' + todt.replace('-', '') + '.xml')
    print("Archiving request at {0}".format(outfile))
    # Convert elements to rows in dataframe
    with open(outfile, 'w') as f:
        f.write(response.text)


def request_changes(fromdt, todt=None, scope='knb-lter-jrn',
    dedup = True, parsedt=False):
    """
    Request PASTA change records in specified temporal range and parse
    into a dataframe.

    Parameters
    ----------
    fromdt : string
        Starting datetime for the request (YYYY-MM-DD)
    todt : string, optional
        Ending datetime for the request (YYYY-MM-DD), by default None
    scope : str, optional
        EDI scope to request changes for, by default 'knb-lter-jrn'
    dedup : bool, optional
        Flag to remove duplicates, by default True
    parsedt : bool, optional
        Flag to parse 'date' field to datetime index, by default False
    """
    # An element tree will be returned from the api request
    print('Requesting PASTA changes for {0} from {1} to {2}'.format(
        scope, fromdt, todt))
    response = rq.recent_changes(scope, fromdt, todt)
    root = rq.response_to_ET(response)
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

def counts_to_daily(df, fromdt = None, todt = None):
    """
    Create a daily dataframe with updates, creates, and total packages.

    Parameters
    ----------
    df : pandas dataframe
        Pandas dataframe derived from 'get_counts()'
    fromdt : string, optional
        Starting datetime for the request (YYYY-MM-DD), by default None
    todt : string, optional
        Ending datetime for the request (YYYY-MM-DD), by default None
    """
    # Create datetime index first if needed
    df2 = df.copy()
    if not isinstance(df2.index, pd.DatetimeIndex):
        df2.index = pd.to_datetime(df2['date'])
        #, format='%Y-%b-%dT%H:%M:%S.%f')
    if fromdt is None:
        fromdt = min(df2.index)
    if todt is None:
        todt = max(df2.index)
    df_out = df2.loc[fromdt:todt,['n_update','n_create','n_tot']].resample(
            'D').sum()
    return(df_out)

