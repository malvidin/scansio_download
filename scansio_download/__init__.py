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
        self.local_catalog_file = "local_catalog.json"

    def download_catalog(self):
        # Download catalog from remote URL
        # returns loaded JSON of catalog, or False
        catalog_response = requests.get(self.catalog_url)
        if catalog_response.status_code == 200:
            catalog_json = json.loads(catalog_response.text)
            return catalog_json
        else:
            return False

    def write_json_catalog(self, local_catalog_json):
        # write catalog to disk with json.dumps
        with open(local_catalog_file, "w") as catalog_file:
            catalog_json_dump = json.dump(local_catalog_json, catalog_file)

    def load_json_catalog(self):
        # returns loaded JSON of catalog, or False
        if os.path.isfile(local_catalog_file):
            with open(local_catalog_file, "r") as catalog_file:
                local_catalog_json = json.load(catalog_file)
        else:
            local_catalog_json = {}
            local_catalog_json["studies"] = []
        return local_catalog_json

    # def load_elasticsearch_catalog(self):
        # # returns loaded Elastic Search version of catalog, or False
        # # Elastic Search capabilities not implemented yet
        # es = Elasticsearch(...)
        # local_catalog_elasticsearch = es.parse()
        # return local_catalog_elasticsearch
        # pass

    def download_latest_study(self, study_id):
        # downloads the most recent study, using the uniqid from https://scans.io/json
        # If specified, validates the SHA1 hash against the fingerprint from Scans.io
        # If the file was already downloaded and validated, returns True
        # Also returns True if the file is downloaded
        current_catalog = self.download_catalog()
        if current_catalog:
            # Get URL and hash from current catalog
            for study in current_catalog["studies"]:
                if study["uniqid"] == study_id:
                    study_file_info = study["files"][-1]
                    study_link = study_file_info["name"]
                    study_hash = study_file_info["fingerprint"]
                    study_filename = study_link.split("/")[-1]
                    # Copy the shell in case the study doesn't exist in the local catalog
                    newest_study = study
                    study_found = True
        if not study_found:
            print("study not found")
            return False  # study not found
        else:
            print("couldn't download the catalog")
            return False  # couldn't download the catalog
        # Compare against local catalog
        if self.local_catalog == "json":
            previous_catalog = self.load_json_catalog()
        # elif self.local_catalog == "elasticsearch":
            # previous_catalog = load_elasticsearch_catalog()
        else:
            print("invalid catalog type specified")
            return False  # invalid catalog type specified
        for study in previous_catalog["studies"]: 
            if study["uniqid"] == study_id:
                for file in study["files"]:
                    if file["fingerprint"] == study_hash:
                        print("file already exists")
                        return True  # file already exists
        #download file
        self.download_large_file(study_link, study_filename)
        if self.validate:
            observed_hash = self.generate_sha1_hash(study_filename)
            if observed_hash == study_hash:
                # Hash verified
                # add download/verification to the local catalog
                study_file_info["verified"] = True
            else:
                print("hash verification failed")
                return False
        # add any validation info to the local catalog
        current_catalog = previous_catalog
        for study in current_catalog:
            if study["uniqid"] == study_id:
                study["files"].append(study_file_info)
                appended = True
        if not appended:
            # add studies shell and file info to JSON.
            current_catalog["studies"] = newest_study
            study["files"].append(study_file_info)
        self.write_json_catalog(current_catalog)  # previous

    def download_large_file(self, download_link, target_file):
        file_download = requests.get(download_link, stream=True)
        with open(target_file, 'wb') as file_target:
            for chunk in file_download.iter_content(chunk_size=1024):
                if chunk:
                    file_target.write(chunk)

    def generate_sha1_hash(self, target_file):
        # calculate the sha1 hash of a file
        with open(target_file, 'rb') as file:
            h = hashlib.sha1()
            while True:
                data = fh.read(8192)
                if not data:
                    break
                h.update(data)
        sha1 = h.hexdigest()
        return sha1

