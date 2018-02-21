# scansio_download
Download files listed on the Internet-Wide Scan Data Repository, https://scans.io/

##Description
Downloads files from the scan repository and tracks successfully downloaded files in a JSON file 
or with Elastic Search. The JSON file mirrors the structure of the scan repository.

Optionally, sends the downloaded file names to a parsing function.

##Examples

Basic download and add to JSON catalog
1. Create instance of JSON catalog class
2. Create instance of Download class
3. Download the latest study using the study's unique ID
    1. Use optional filter for studies that release multiple study types
4. The returned value is the filename that was successfully downloaded, or False
    1. If successfully downloaded, the study and file metadata is stored in the JSON catalog
    
```
catalog = scansio.JSONCatalog()
dl = scansio.Download()
newest_study = dl.download_latest_study('scott-top-one-million', catalog, 'umbrella')
```

Download latest 5 studies and add to Elastic Search catalog
1. Create instance of Elastic Search catalog class
    1. Include a parsing function to index the file contents in Elastic Search (study_parser in example below)
        1. Takes the filename as the only parameter
        2. Must return True to add downloaded files to the catalog index
2. Create instance of Download class
3. Download the most recent 5 studies using the study's unique ID
    1. Use optional filter for studies that release multiple study types
4. The returned value is a list of the filenames that were successfully downloaded, or False
    1. If successfully downloaded, the study and file metadata is indexed in the Elastic Search catalog

```
import scansio.esindex
from elasticsearch import ElasticSearch
import gzip

def study_parser(filename):
    with gzip.open(filename, 'rb') as f:
        file_content = f.read()
        return True
    return False
    
esconnection = Elasticsearch([{'host': '127.0.0.1', 'port': 443}], timeout=20)
catalog = scansio.esindex.ESCatalog(esconnection, study_parser)
dl = scansio.Download()
newest_study_list = dl.download_study_files('scott-top-one-million', catalog, -5, 'umbrella')
```
