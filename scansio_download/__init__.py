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
        with open(self.local_catalog_file, "w") as catalog_file:
            catalog_json_dump = json.dump(local_catalog_json, catalog_file)

    def load_json_catalog(self):
        # returns loaded JSON of catalog, or False
        if os.path.isfile(self.local_catalog_file):
            with open(self.local_catalog_file, "r") as catalog_file:
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
        # If the file is downloaded, returns the downloaded file's name
        current_catalog = self.download_catalog()
        study_found = False
        if current_catalog:
            # Get URL and hash from current catalog
            for study in current_catalog["studies"]:
                # TODO Sort by creation date, the sort order is not consistent
                if study["uniqid"] == study_id:
                    study["files"].sort(key=date_to_int, reverse=True)
                    study_file_info = study["files"][0]
                    study_link = study_file_info["name"]
                    study_hash = study_file_info["fingerprint"]
                    study_filename = study_link.split("/")[-1]
                    # Copy the shell in case the study doesn't exist in the local catalog
                    newest_study = study
                    newest_study["files"] = [ study_file_info ]
                    # print newest_study
                    study_found = True
        if not study_found:
            print("study not found")
            return False  # study not found
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
        # download file
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
        appended = False
        for study in current_catalog["studies"]:
            if study["uniqid"] == study_id:
                study["files"].append(study_file_info)
                appended = True
        if not appended:
            # add studies shell and file info to JSON.
            current_catalog["studies"] =  [ newest_study ]
            study["files"].append(study_file_info)
        self.write_json_catalog(current_catalog)  # previous
        return study_filename

    @staticmethod
    def download_large_file(download_link, target_file):
        file_download = requests.get(download_link, stream=True)
        with open(target_file, 'wb') as file_target:
            for chunk in file_download.iter_content(chunk_size=1024):
                if chunk:
                    file_target.write(chunk)

    @staticmethod
    def generate_sha1_hash(target_file):
        # calculate the sha1 hash of a file
        with open(target_file, 'rb') as file_target:
            h = hashlib.sha1()
            while True:
                data = file_target.read(8192)
                if not data:
                    break
                h.update(data)
        sha1 = h.hexdigest()
        return sha1
    
    @staticmethod
    def date_to_int(json):
        try:
            print json["updated-at"].replace("-","")
            return int(json["updated-at"].replace("-",""))
        except KeyError:
            return 0
