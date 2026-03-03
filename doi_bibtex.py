## Written by Claude, prompted and lightly edited by Greg
import requests
import pyEDIutils.pasta_api_requests as rq
from urllib.parse import urlparse, parse_qs
import time


def parse_portal_url(url):
    """Parse scope and identifier from an EDI NIS portal mapbrowse URL.

    Parameters
    ----------
    url : str
        An EDI NIS portal mapbrowse URL in the form:
        https://portal.edirepository.org/nis/mapbrowse?scope=...&identifier=...

    Returns
    -------
    tuple
        (scope, identifier) as strings
    """
    parsed = urlparse(url.strip())
    params = parse_qs(parsed.query)
    return params["scope"][0], params["identifier"][0]


def pkg_doi(scope, identifier):
    """Fetch the DOI for the newest revision of a PASTA data package.

    Calls pkg_revisions to determine the current revision, then requests
    the DOI from the PASTA data package manager API.

    Parameters
    ----------
    scope : str
        EDI scope string (e.g., "knb-lter-jrn")
    identifier : str or int
        Data package identifier number

    Returns
    -------
    str or None
        DOI string (e.g., "doi:10.6073/pasta/..."), or None if not found
    """
    # Get the newest revision number for this package
    revision = rq.pkg_revisions(identifier, scope=scope, filt="newest").strip()

    # Request the DOI from PASTA
    rq_url = "https://pasta.lternet.edu/package/doi/eml/" + "/".join(
        [scope, str(identifier), revision]
    )
    response = requests.get(rq_url)
    print(response.request.url)

    if response.status_code == 200:
        return response.text.strip()
    else:
        print(
            f"  Warning: no DOI for {scope}.{identifier}.{revision}"
            f" (HTTP {response.status_code})"
        )
        return None


def doi_to_bibtex(doi):
    """Fetch a BibTeX entry for a DOI using content negotiation via doi.org.

    Parameters
    ----------
    doi : str
        DOI string, with or without the "doi:" prefix

    Returns
    -------
    str or None
        BibTeX entry string, or None if the request failed
    """
    # Strip the "doi:" prefix if present, then build the doi.org URL
    doi_clean = doi.replace("doi:", "").strip()
    response = requests.get(
        f"https://doi.org/{doi_clean}",
        headers={"Accept": "application/x-bibtex"},
        allow_redirects=True,
    )

    if response.status_code == 200:
        return response.text.strip()
    else:
        print(f"  Warning: BibTeX not found for {doi} (HTTP {response.status_code})")
        return None


def dois_from_url_list(urls, delay=0.5):
    """Fetch DOIs for a list of EDI portal mapbrowse URLs.

    Parameters
    ----------
    urls : list of str
        EDI NIS portal mapbrowse URLs
    delay : float, optional
        Seconds to wait between API requests, by default 0.5

    Returns
    -------
    list of tuples
        (scope, identifier, doi) for each URL; doi is None if not found
    """
    results = []
    for url in urls:
        url = url.strip()
        if not url:
            continue
        scope, identifier = parse_portal_url(url)
        print(f"Fetching DOI for {scope}.{identifier}...")
        doi = pkg_doi(scope, identifier)
        results.append((scope, identifier, doi))
        time.sleep(delay)
    return results


def bibtex_from_url_file(infile, outfile, delay=0.5):
    """Read EDI portal URLs from a text file, fetch DOIs and BibTeX entries,
    and write a .bib file.

    Parameters
    ----------
    infile : str
        Path to a text file with one EDI portal mapbrowse URL per line
    outfile : str
        Path for the output .bib file
    delay : float, optional
        Seconds to wait between API requests, by default 0.5
    """
    # Read and filter blank lines
    with open(infile, "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    print(f"Processing {len(urls)} URLs from {infile}")

    # Fetch a DOI for each URL
    doi_results = dois_from_url_list(urls, delay=delay)

    # Fetch BibTeX for each DOI and write to the output file
    n_success = 0
    with open(outfile, "w") as f:
        for scope, identifier, doi in doi_results:
            if doi is None:
                print(f"  Skipping {scope}.{identifier} (no DOI)")
                continue
            print(f"Fetching BibTeX for {doi}...")
            bibtex = doi_to_bibtex(doi)
            if bibtex:
                f.write(bibtex + "\n\n")
                n_success += 1
            time.sleep(delay)

    print(f"Wrote {n_success} BibTeX entries to {outfile}")
