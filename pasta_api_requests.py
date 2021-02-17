import requests
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

def get_pkg_revision(pkgid=None, scope='knb.lter.jrn', filt='newest'):
    """
    Request the newest package revision number
    """
    url = 'https://pasta.lternet.edu/package/eml/' + scope + '/' + pkgid

    params = (
        ('filter', filt)
    )
    response = requests.get(url, params=params)

    return response.text
