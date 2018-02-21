"""
Interacts with an Elastic Search index
"""

import logging
from elasticsearch import Elasticsearch
from datetime import datetime


class ESCatalog:
    """ Interact with Elastic Search"""

    def __init__(self, esconnection, parser=lambda x: True):
        # API keys, index, etc.
        self.esconnection = esconnection
        self.parser = parser
        assert isinstance(esconnection, Elasticsearch)

    def write(self, study, study_file_info, study_index='scansio-imported'):
        # study is the study metadata, minus the file objects
        # study_file_info is one member of the file information list
        study_id = study['uniqid']
        study_filename = study_file_info['name'].split("/")[-1]
        study_hash = study_file_info['fingerprint']
        # Clean up study ID
        study_id = study_id.replace(".", "-")
        # if file is indexed successfully, add to Elastic Search index
        if self.parser(study_filename) == True:
            logging.info('parser success (if defined)')
            logging.info('attempting write to Elastic Search catalog')
            self.esconnection.index(index=study_index, doc_type='imported-file', id=study_filename,
                                    body={'study': study_id, 'file': study_filename,
                                          'imported_date': datetime.now(), 'sha1': study_hash})
        else:
            logging.error("parser failed, not writing to catalog")

    def contains(self, study_hash, study_index='scansio-imported'):
        # Determine if Elastic Search already contains the file
        search_result = self.esconnection.search(index=study_index, body={'query': {'term': {'sha1': study_hash}}})
        if study_hash in search_result:
            logging.info("%s already exists in local catalog", study_hash)
            return True
        else:
            logging.info("%s not found in local catalog", study_hash)
            return False
