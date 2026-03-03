# pyEDIutils

Python utilities for interacting with EDI's PASTA servers. Uses python requests library and converts responses to pandas dataframes. See examples of use in the JRN reporting repository.


**Running the BibTex generator**

    import pyEDIutils.doi_bibtex as db

    db.bibtex_from_url_file(
        "pyEDIutils/2025AR_unique_EDI_creates_updates_20260227.txt",
        "pyEDIutils/2025AR_EDI_citations.bib"
    )
