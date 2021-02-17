import pyEDIutils.pasta_api_requests as rq
import pandas as pd
import os
import pdb

    
def searchroot_to_df(root, fields):
    """
    Convert a returned search Element Tree object to a dataframe.
    Each element becomes a row with columns in the fields argument.
    Keyword fields are ignored.
    """
    dfill = {}
    for f in fields:
        if (f=='keyword' or f=='author'):
            dfill[f+'s'] = [';'.join([v.text for v in vs.iter(f)]) 
                    for vs in root.iter(f+'s')]
        else:
            dfill[f] = [v.text for v in root.iter(f)]
    # This fails on keyword or authors fields
    #dfill = {f: [g.text for g in root.iter(f)] for f in fields
    #        if 'keyword' not in f}
    df = pd.DataFrame(dfill)
    return(df)
                      

        
def request_search(query='scope:knb-lter-jrn',
        fields=['packageid','doi','title','pubdate'],
        sortby='packageid,asc', rows=500):
    """
    Get packages and relevant fields for a given scope. Uses the Apache
    solr search in PASTA:
    https://lucene.apache.org/solr/guide/8_8/query-syntax-and-parsing.html

    Example:

    df = edi.request_search(query=['scope:knb-lter-jrn', 'author:Peters',
                               '-packageid:*.210308*'],
                        fields=('packageid','title','pubdate','keyword',
                                'author','begindate','enddate','doi'),
                       sortfields='packageid,desc')
    """
    # fq must be a 'field:queryterm' or list of 'field:queryterm'
    # possible fields are 'scope', 'author', 'title', 'packageid', etc
    # queryterms can be prefixed with '-' to exclude and use '*' wildcards
    fq = query
    # Fields to return from pasta - solr needs comma or space separated list
    fl = ','.join(fields)
    # Fields to sort on (follow with ',asc' or ',desc'
    sort = sortby
    # An element tree will be returned from the api request
    root = rq.search_request(fq, fl, sort, rows)
    # Convert elements to rows in dataframe
    df_out = searchroot_to_df(root, fields)
    return(df_out)

