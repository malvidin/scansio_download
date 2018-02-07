import os
import requests
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

    def download(self, study_name):
        # downloads the most recent study, using the uniqid from https://scans.io/json
        # If specified, validates the SHA1 hash against the fingerprint from Scans.io
        pass
