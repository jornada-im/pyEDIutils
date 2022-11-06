import pyEDIutils.pasta_api_requests as rq
import pandas as pd
import os
import pdb

    
def searchroot_to_df(root, fields):
    """Convert PAST solr search result to a dataframe

    Convert a returned ElementTree object from a PASTA solr search to a
    dataframe. Each element becomes a row with columns in the fields argument.
    Multifields, like keywords and authors, are joined as semicolon-delimited
    lists in the resulting dataframe column.

    Parameters
    ----------
    root : ElementTree object
        An ElementTree object returned from a PASTA solr search
    fields : string list
        A list of field names PASTA returned in the query, which will become
        columns in the dataframe
    """
    dfill = {}
    for f in fields:
        if (f=='keyword' or f=='author'):
            dfill[f+'s'] = [';'.join([v.text for v in vs.iter(f)]) 
                    for vs in root.iter(f+'s')]
        elif f=='coordinates':
            # The solr search sends back a set of coordinates for each
            # geographicCoverage element, so if there are multiple elements
            # fdthis will create a column that exceeds the number of datasetids
            # Not sure how to parse these multiple returns yet, so just
            # counting them here.
            #dfill[f] = [';'.join([v.text for v in vs.iter(f)]) 
            #        for vs in root.iter(f)]
            print('More than 1 spatial entity per packageid, so just counting')
            dfill[f + '_ent'] = [len(sc.findall('coordinates'))
                for sc in root.iter('spatialCoverage')]
        else:
            dfill[f] = [v.text for v in root.iter(f)]
    # Make a dataframe from dfill
    df = pd.DataFrame(dfill)
    return(df)
                      

        
def search_pasta(query='scope:knb-lter-jrn',
        fields=['packageid','doi','title','pubdate'],
        sortby='packageid,asc', rows=500, returnroot=False):
    """Search packages in PASTA
    
    This searches packages across a variety of fields using the Apache solr
    search in PASTA:
    https://lucene.apache.org/solr/guide/8_8/query-syntax-and-parsing.html

    Example:

    df = edi.request_search(query=['scope:knb-lter-jrn', 'author:Peters',
                               '-packageid:*.210308*'],
                        fields=('packageid','title','pubdate','keyword',
                                'author','begindate','enddate','doi'),
                       sortby='packageid,desc')

    Parameters
    ----------
    query : str or list of strings, optional
        A string or list of query terms (field:term), by default 'scope:knb-lter-jrn'
    fields : list, optional
        List of fields to return from the search, by default 
        ['packageid','doi','title','pubdate']
    sortby : str, optional
        List of  fields to sort result by, by default 'packageid,asc'
    rows : int, optional
        number of rows to return, by default 500
    returnroot : bool, optional
        Flag to return ElementTree root instead of dataframe, by default False

    Returns
    -------
    dataframe or (dataframe, root)
        Returns either a dataframe or a dataframe and ElementTree root object
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
    response = rq.pasta_solr_search(fq, fl, sort, rows)
    root = rq.response_to_ET(response)
    # Convert elements to rows in dataframe
    df_out = searchroot_to_df(root, fields)
    if returnroot:
        return (df_out, root)
    else:
        return df_out

