import pyEDIutils.pasta_api_requests as rq
import pandas as pd
from datetime import date
import os
import pdb
    
def auditroot_to_df(ediroot):
    """
    Convert an Element Tree object to a dataframe. Each element has
    date, packageid, and service method extracted into a row in the 
    dataframe.
    """
    # Iterate over each element in ediroot and extract the variables
    print(ediroot.text)
    df = pd.DataFrame({'scope':[scope.text for scope in ediroot.iter('scope')],
        'identifier':[int(ident.text) for ident in ediroot.iter('identifier')],
        'revision':[int(rev.text) for rev in ediroot.iter('revision')],
        'resource':[rtype.text for rtype in ediroot.iter('resourceType')],
        'total_reads':[int(tot.text) for tot in ediroot.iter('totalReads')],
        'non_robot_reads':[int(nrr.text) for nrr 
                           in ediroot.iter('nonRobotReads')]}
        )
    return(df)

def auditreport_to_df(ediroot):
    """
    Convert an Element Tree object to a dataframe. Each element has
    date, packageid, and service method extracted into a row in the 
    dataframe.
    """
    # Iterate over each element in ediroot and extract the variables
    print(ediroot.text)
    df = pd.DataFrame({
        'entry_dt':[etime.text for etime in ediroot.iter('entryTime')],
        'method':[meth.text for meth in ediroot.iter('serviceMethod')],
        'resource_id':[rid.text for rid in ediroot.iter('resourceId')],
        'user':[user.text for user in ediroot.iter('user')],
        'group':[groups.text for groups in ediroot.iter('groups')],
        'useragent':[agent.text for agent in ediroot.iter('userAgent')]}
        )
    return(df)

def request_audit(identifier, rev=None, scope='knb-lter-jrn'):
    """Generate an audit report for a document or data package

    Parameters
    ----------
    identifier : [type]
        [description]
    rev : [type], optional
        [description], by default None
    scope : str, optional
        [description], by default 'knb-lter-jrn'
    """
    # An element tree will be returned from the api request
    if rev is not None:
        print('Requesting access data for {0}.{1}.{2}'.format(scope,
            identifier, rev))
        response = rq.aud_package(identifier, rev=rev, scope=scope)
        root = rq.response_to_ET(response)
    else:
        print('Requesting access data for {0}.{1}'.format(
            scope, identifier))
        response = rq.aud_document(identifier, scope)
        root = rq.response_to_ET(response)
    
    # Convert elements to rows in dataframe
    df_out = auditroot_to_df(root)

    return(df_out)



def request_audit_report(servmethod, dn, pw, user=None, group=None,
                       resid='knb-lter-jrn', fromdt=date.today(), todt=None,
                       lim=10000):
    """Get an audit report from PASTA+

    Parameters
    ----------
    servmethod : [type]
        [description]
    df : [type]
        [description]
    pw : [type]
        [description]
    user : [type], optional
        [description], by default None
    group : [type], optional
        [description], by default None
    resid : str, optional
        [description], by default 'knb-lter-jrn'
    fromdt : [type], optional
        [description], by default date.today()
    todt : [type], optional
        [description], by default None
    lim : int, optional
        [description], by default 10000
    """
    # An element tree will be returned from the api request
    print('Requesting audit report for {0} starting {1}'.format(resid, fromdt))
    response = rq.aud_report_dpm(servmethod, user, group, resid, fromdt, todt, lim,
                   dn, pw)
    root = rq.response_to_ET(response)
    # Convert elements to rows in dataframe
    df_out = auditreport_to_df(root)

    return(df_out)
