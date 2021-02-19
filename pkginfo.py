import pyEDIutils.pasta_api_requests as rq
import pandas as pd
import pdb

def entity_table(scope, identifier, revision):
    """
    Create a table describing entities attached to a data package. Calls first
    for a listing of entities for the package, then loops through each of 
    those, requests metadata for each, and assembles everything into a table.

    Returns a dataframe
    """
    # Request a list of data entities (returns a dataframe)
    df = rq.pkg_entity_names(scope, identifier, revision)
    # Add some columns to dataframe
    df['packageid'] = '.'.join([scope, identifier, revision])
    df['entityorder'] = df.index + 1
    df['filename'] = 'NA'
    df['entitytype'] = 'otherEntity'
    df['filetype'] = None
    # Loop through entity ids and add filename, entitytypes and filetypes.
    for i, e in enumerate(df.entityid):
        # request metadata for the entity
        metaroot = rq.pkg_entity_metadata(scope, identifier, revision, e)
        df.loc[i,'filename'] = metaroot.find('./fileName').text
        if metaroot.find('./dataFormat').text=='text/csv':
                df.loc[i,'entitytype'] = 'dataTable'
                df.loc[i, 'filetype'] = 'csv_D'
    # Return the dataframe
    return df
