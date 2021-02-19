import requests
from requests.compat import urljoin
import xml.etree.ElementTree as ET
import pandas as pd
import os
import pdb

def load_xml(xmlname):
    """
    Load a stored request xml file
    """
    tree = ET.parse(xmlname)
    root = tree.getroot()
    return(root)


def search_request(fqs, fls, sort, rows, env='production'):
    """
    This is the _search data packages_ API call.

    curl -i -X GET "https://pasta.lternet.edu/package/search/eml?defType=edismax&q=*&fq=scope:knb-lter-jrn&fl=packageid,doi,keyword,title,begindate,enddate,pubdate&rows=700"

    See PASTA docs: https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#search-data-packages

    Also see the Apache Solr docs: https://lucene.apache.org/solr/guide/
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
    root = ET.fromstring(response.text)
    return(root)

def recent_changes_request(scope, fromdt, todt=None):
    """
    This is the _list recent changes_ call.

    curl -i -X GET 'https://pasta.lternet.edu/package/changes/eml?fromDate=2017-02-01T12:00:00&toDate=2020-01-28&scope=knb-lter-jrn'

    see: https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#list-recent-changes
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
    root = ET.fromstring(response.text)
    return(root)

def pkg_entity_names(scope, identifier, revision):
    """
    Request entity identifiers and names for a specified data package.

    Note that PASTA returns a csv-like text object, but some entity names have
    commas, so they won't parse with pandas read_csv. This parses and
    returns a dataframe.
    
    API documentation:

    https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#read-data-entity-name
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
    """
    Get entity names/identifiers for a specified data package.

    PASTA returns an XML tree with metadata for the entity. This function
    returns the ElementTree root derived from it.

    https://pastaplus-core.readthedocs.io/en/latest/doc_tree/pasta_api/data_package_manager_api.html#read-data-entity-resource-metadata
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/package/data/rmd/eml/'
    rq_url = urljoin(base_url, '/'.join([scope, identifier,
        revision, entityid]))
    # Request
    response = requests.get(rq_url)
    # Print out the request url and return ET root
    print(response.request.url)
    root = ET.fromstring(response.text)
    return(root)


def pkg_revisions(identifier, scope='knb-lter-jrn', filt='newest'):
    """
    Request the package revision numbers for a package in PASTA.
    Change filt to None to get all revisions in PASTA
    """
    # Create the URL
    base_url = 'https://pasta.lternet.edu/package/eml/'
    rq_url = urljoin(base_url, '/'.join([scope, identifier]))
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
