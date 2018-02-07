import os
import requests
import json
import hashlib
# from elasticsearch import Elasticsearch


class Download:
    """ Download files from scans.io """

    def __init__(self, catalog_url="https://scans.io/json", validate=True, local_catalog="json"):
        # The local catalog is currently a JSON file with similar structure to the Scans.io JSON catalog
        # Should be able to validate against Elastic Search in the future
        self.validate = validate
        self.catalog_url = catalog_url
        self.local_catalog = local_catalog

    def download_catalog(self):
        # Download catalog from remote URL
        # returns loaded JSON of catalog or False
        catalog_response = requests.get(self.scans_io_catalog)
        if catalog_response.text == 200:
            catalog_json = json.loads(catalog_response.text)
            return catalog_json
        else:
            return False

    def load_json_catalog(self, self.local_catalog):
        # returns loaded JSON of catalog, or False
        if self.local_catalog == "json"
            pass
            return local_catalog_json
        else:
            return False

    def load_elasticsearch_catalog(self, self.local_catalog):
        # returns loaded ElasticSearch version of catalog, or False
        elif self.local_catalog == "elasticsearch":
            # not implemented
            pass
            return local_catalog_elasticsearch
        else:
            return False

    def download_latest_study(self, study_name):
        # downloads the most recent study, using the uniqid from https://scans.io/json
        # If specified, validates the SHA1 hash against the fingerprint from Scans.io
        pass
