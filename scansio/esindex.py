"""*
Interacts with an Elastic Search index
"""

from elasticsearch import Elasticsearch
from datetime import datetime


class ESCatalog:
    """ Interact with Elastic Search"""

    def __init__(self, host, port, timeout=30):
        # API keys, index, etc.
        self.host = host
        self.port = port
        self.timeout = timeout
        self.esconnection = Elasticsearch([{'host': host, 'port': port}], timeout=timeout)

    def write(self, study, study_file_info, study_index='scansio-imported'):
        # study is the study metadata, minus the file objects
        # study_file_info is one member of the file information list
        study_id = study['uniqid']
        study_filename = study_file_info['name'].split("/")[-1]
        study_hash = study_file_info['fingerprint']
        # Clean up study ID
        study_id = study_id.replace(".", "-")
        # Add file to Elastic Search, checking compression and file type
        insert_success = True
        # if file is indexed successfully, add to Elastic Search index
        if insert_success:
            self.esconnection.index(index=study_index, doc_type='imported-file', id=study_filename,
                                    body={'study': study_id, 'file': study_filename,
                                          'imported_date': datetime.now(), 'sha1': study_hash})

    def contains(self, study_hash, study_index='scansio-imported'):
        # Determine if Elastic Search already contains the file
        search_result = self.esconnection.search(index=study_index, body={"size": 3000, "query": {"sha1": {study_hash}}})
        if study_hash in search_result:
            return True
        else:
            return False
        pass
