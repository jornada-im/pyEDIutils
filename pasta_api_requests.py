import requests
from requests.compat import urljoin
import xml.etree.ElementTree as ET
import pandas as pd
import os

def archived_response_to_ET(xmlname):
    """
    Load an archived python request xml file and return it as an ElementTree

    Parameters
    ----------
    xmlname : string
        filename and path to a stored response file
    """
    tree = ET.parse(xmlname)
    root = tree.getroot()
    return(root)

def response_to_ET(response):
    """
    Return the response from a PASTA request as an ElementTree

    Parameters
    ----------
    response : string
        A response from a python request to the PASTA API
    """
    root = ET.fromstring(response.text)
    return(root)


def pasta_solr_search(fqs, fls, sort, rows, env='production'):
    """
    Make python requests to the PASTA _search data packages_ API call.

    curl -i -X GET "https://pasta.lternet.edu/package/search/eml?defType=edismax&q=*&fq=scope:knb-lter-jrn&fl=packageid,doi,keyword,title,begindate,enddate,pubdate&rows=700"

    See PASTA docs: https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#search-data-packages

    Also see the Apache Solr docs: https://lucene.apache.org/solr/guide/

    Parameters
    ----------
    fqs : string or string list
        fq must be a 'field:queryterm' or list of 'field:queryterm'
        possible fields are 'scope', 'author', 'title', 'packageid', etc
        queryterms can be prefixed with '-' to exclude and use '*' wildcards
    fls : string or string list
        Fields to return from pasta - solr needs comma or space separated list
    sort : string or string list
        Fields to sort on (follow with ',asc' or ',desc'
    rows : int
        Number of rows to include in response
    env : str, optional
        PASTA environment to search, by default 'production'
    """
    if env=='staging':
        base_url = 'https://pasta-s.lternet.edu/package/search/eml'
    else:
        base_url = 'https://pasta.lternet.edu/package/search/eml'
    
    params = (
        ('defType', 'edismax'),
        ('q','*'),
        ('fq', fqs),
        ('fl', fls),
        ('sort', sort),
        ('rows', rows))
    response = requests.get(base_url, params=params)
    # Print out the request url
    print(response.request.url)
    return(response)

def recent_changes(scope, fromdt, todt=None):
    """The _list recent changes_ PASTA API request. It returns an xml
    response populated with operations in the PASTA database.

    curl -i -X GET 'https://pasta.lternet.edu/package/changes/eml?fromDate=2017-02-01T12:00:00&toDate=2020-01-28&scope=knb-lter-jrn'

    see: https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#list-recent-changes

    Parameters
    ----------
    scope : str
        EDI scope for the request
    fromdt : string
        Starting datetime for the request (YYYY-MM-DD)
    todt : string, optional
        Ending datetime for the request (YYYY-MM-DD), by default None
    """
    if todt is None:
        from datetime import datetime
        todt = datetime.today().strftime('%Y-%m-%d')
    base_url = 'https://pasta.lternet.edu/package/changes/eml'
    params = (
        ('fromDate', fromdt),
        ('toDate', todt),
        ('scope', scope))
    response = requests.get(base_url, params=params)
    # Print out the request url
    print(response.request.url)
    return(response)

def pkg_entity_names(scope, identifier, revision):
    """Request entity identifiers and names for a specified data package.

    Note that PASTA returns a csv-like text object, but some entity names have
    commas, so they won't parse with pandas read_csv (using StringIO, for ex.).
    This function parses the text object and returns a dataframe.
    
    API documentation:

    https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#read-data-entity-name

    Parameters
    ----------
    scope : string
        EDI scope for the request
    identifier : int
        Data package identifier
    revision : int
        Revision number of the data package
    """
    # Create URL
    base_url = 'https://pasta.lternet.edu/package/name/eml/'
    rq_url = urljoin(base_url, '/'.join([scope, identifier, revision]))
    # Request
    response = requests.get(rq_url)
    # Print out the request url
    print(response.request.url)
    # Parse the csv and return a dataframe
    l1 = response.text.split('\n')[0:-1]
    l2 = [l1[i].split(',', 1) for i in range(0, len(l1))]
    df = pd.DataFrame(l2, columns = ('entityid', 'entityname'))
    return(df)

def pkg_entity_metadata(scope, identifier, revision, entityid):
    """Get entity names/identifiers for a specified data package. Entityid is
    the identifier hash for the entity in PASTA, which can be returned using
    the pkg_entity_names function.

    PASTA returns an XML tree with metadata for the entity. This function
    returns the ElementTree root derived from it.

    https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#read-data-entity-resource-metadata


    Parameters
    ----------
    scope : string
        EDI scope for the request
    identifier : int
        Data package identifier
    revision : int
        Revision number of the data package
    entityid : string
        identifier hash for the entity in PASTA
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/package/data/rmd/eml/'
    rq_url = urljoin(base_url, '/'.join([scope, identifier,
        revision, entityid]))
    # Request
    response = requests.get(rq_url)
    # Print out the request url and return ET root
    print(response.request.url)
    
    return(response)


def pkg_revisions(identifier, scope='knb-lter-jrn', filt='newest'):
    """Request the package revision numbers for a package in PASTA.
    
    Change filt to None to get all revisions in PASTA

    Parameters
    ----------
    identifier : int
        Data package identifier
    scope : str, optional
        EDI scope for the request, by default 'knb-lter-jrn'
    filt : str, optional
        _description_, by default 'newest'

    Returns
    -------
    _type_
        _description_
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/package/eml/'
    rq_url = urljoin(base_url, '/'.join([scope, str(identifier)]))
    # Parameters
    params = (
        ('filter', filt),
    )
    # Request, print return
    response = requests.get(rq_url, params=params)
    print(response.request.url)
    if filt is not None:
        return response.text
    else:
        return response.text.split('\n')

def aud_document(identifier, scope='knb-lter-jrn'):
    """Get an audit report for access to a document (scope.identifier)

    Parameters
    ----------
    identifier : int
        Data package identifier
    scope : str, optional
        EDI scope for the request, by default 'knb-lter-jrn'

    Returns
    -------
    [type]
        [description]
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/audit/reads/'
    rq_url = urljoin(base_url, '/'.join([scope, str(identifier)]))
    # Request, print return
    response = requests.get(rq_url)#, params=params)
    print(response.request.url)
    return(response)


def aud_package(scope, identifier, revision):
    """Get an audit report for access to a datapackage (scope.identifier.rev)

    Parameters
    ----------
    scope : string
        EDI scope for the request
    identifier : int
        Data package identifier
    revision : int
        Revision number of the data package

    Returns
    -------
    [type]
        [description]
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/audit/reads/'
    rq_url = urljoin(base_url, '/'.join([scope, str(identifier), str(rev)]))
    # Request print return
    response = requests.get(rq_url)#, params=params)
    print(response.request.url)
    return(response)


def aud_report_dpm(servmethod, user, group, resid, fromdt, todt, lim,
                   dn, pw):
    """Get an audit report from the PASTA data package manager

    Parameters
    ----------
    servmethod : [type]
        [description]
    user : [type]
        [description]
    group : [type]
        [description]
    resid : [type]
        [description]
    fromdt : [type]
        [description]
    todt : [type]
        [description]
    lim : [type]
        [description]
    dn : [type]
        [description]
    pw : [type]
        [description]
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/audit/report/'
    rq_url = base_url
    # Parameters
    params = (
        ('category', 'info'),
        ('service', 'DataPackageManager-1.0'),
        ('serviceMethod', servmethod),
        ('user', user),
        ('group', group),
        ('authSystem', 'https://pasta.edirepository.org/authentication'),
        ('resourceId', resid),
        ('fromTime', fromdt),
        ('toTime', todt),
        ('limit', lim)
    )
    # Request, print return
    response = requests.get(rq_url, params=params, auth=(dn, pw))
    print(response.request.url)
    return(response)

def aud_count_dpm(servmethod, user, group, resid, fromdt, todt, lim,
                  dn, pw):
    """Get an audit count from the PASTA data package manager

    Parameters
    ----------
    servmethod : [type]
        [description]
    user : [type]
        [description]
    group : [type]
        [description]
    resid : [type]
        [description]
    fromdt : [type]
        [description]
    todt : [type]
        [description]
    lim : [type]
        [description]
    dn : [type]
        [description]
    pw : [type]
        [description]
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/audit/count/'
    rq_url = base_url
    # Parameters
    params = (
        ('category', 'info'),
        ('service', 'DataPackageManager-1.0'),
        ('serviceMethod', servmethod),
        ('user', user),
        ('group', group),
        ('authSystem', 'https://pasta.edirepository.org/authentication'),
        ('resourceId', resid),
        ('fromTime', fromdt),
        ('toTime', todt),
        ('limit', lim)
    )
    # Request, print return
    response = requests.get(rq_url, params=params, auth=(dn, pw))
    print(response.request.url)
    return(response)
