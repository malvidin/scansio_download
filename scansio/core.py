"""
Interacts with the Scans.io catalog, the files it links to, and a local JSON catalog of previously downloaded files
The import of the elastic search module only occurs if the local catalog is 'elasticsearch'
"""


import os
import requests
import json
import hashlib


def download_large_file(download_link, target_file):
    file_download = requests.get(download_link, stream=True)
    with open(target_file, 'wb') as file_target:
        for chunk in file_download.iter_content(chunk_size=1024):
            if chunk:
                file_target.write(chunk)


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


class Download:
    """ Download files from scans.io """

    def __init__(self, catalog_url="https://scans.io/json", local_catalog="json"):
        # The local catalog is currently a JSON file with similar structure to the Scans.io JSON catalog
        # Should be able to validate against Elastic Search in the future"
        self.catalog_url = catalog_url
        self.local_catalog = local_catalog

    def download_catalog(self):
        # Download catalog from remote URL
        # returns loaded JSON of catalog, or False
        catalog_response = requests.get(self.catalog_url)
        if catalog_response.status_code == 200:
            catalog_json = json.loads(catalog_response.text)
            return catalog_json
        else:
            return False

    def download_latest_study(self, study_id):
        # downloads the most recent study, using the uniqid from https://scans.io/json
        # If the file was already downloaded, returns True
        # If the file is downloaded, returns the downloaded file's name
        current_catalog = self.download_catalog()
        study_found = False
        if current_catalog:
            # Get URL and hash from current catalog
            for study in current_catalog["studies"]:
                if study["uniqid"] == study_id:
                    study["files"].sort(key=self.date_to_int, reverse=True)
                    study_file_info = study["files"][0]
                    study_link = study_file_info["name"]
                    study_hash = study_file_info["fingerprint"]
                    study_filename = study_link.split("/")[-1]
                    # Copy the shell in case the study doesn't exist in the local catalog
                    newest_study = study
                    newest_study["files"] = [study_file_info]
                    # print(newest_study)
                    study_found = True
        if not study_found:
            # print("study not found")
            return False  # study not found
        # Set the catalog type
        if self.local_catalog == "json":
            catalog = JSONCatalog()
        elif self.local_catalog == "elasticsearch":
            import esindex  # DON'T WANT THIS CONDITIONAL IMPORT
            catalog = esindex.ESCatalog()
        else:
            print("invalid catalog type specified")
            return False  # invalid catalog type specified
        # Compare against catalog, 
        if catalog.contains(study_hash):
            # print("Found hash (%s) in catalog") % study_hash
            return True
        # download file
        download_large_file(study_link, study_filename)
        observed_hash = generate_sha1_hash(study_filename)
        if observed_hash == study_hash:
            # Hash verified; add verification to the local catalog
            study_file_info["verified"] = True
        else:
            print("hash verification failed, expected %s, observed %s") % study_hash, observed_hash
            return False
        # add any validation info to the local catalog
        previous_catalog = catalog.load()
        current_catalog = previous_catalog
        appended = False
        if self.local_catalog == "json":
            for study in current_catalog["studies"]:
                if study["uniqid"] == study_id:
                    study["files"].append(study_file_info)
                    appended = True
            if not appended:
                # add studies shell and file info to JSON.
                current_catalog["studies"] = [newest_study]
                study["files"].append(study_file_info)
            catalog.write(current_catalog)
        # Place holder for adding information to Elastic Search
        elif self.local_catalog == "elasticsearch":
            catalog.write(current_catalog)
        return study_filename

    @staticmethod
    def date_to_int(json_input):
        try:
            # print(json_input["updated-at"].replace("-",""))
            return int(json_input["updated-at"].replace("-", ""))
        except KeyError:
            # print("unexpected date format")
            return 0


class JSONCatalog:
    """ Interact with local JSON catalog file """

    def __init__(self, local_catalog_file="local_catalog.json"):
        self.local_catalog_file = local_catalog_file

    def load(self):
        # returns loaded JSON of catalog, or False
        if os.path.isfile(self.local_catalog_file):
            with open(self.local_catalog_file, "r") as catalog_file:
                local_catalog_json = json.load(catalog_file)
        else:
            local_catalog_json = {"studies": []}
        return local_catalog_json

    def write(self, local_catalog):
        # write catalog to disk with json.dump
        with open(self.local_catalog_file, "w") as catalog_file:
            json.dump(local_catalog, catalog_file)

    def contains(self, hash_string):
        # Returns True if 'name' or 'fingerprint' of a study is in the local catalog
        # The 'name' is the source URL of the study, the fingerprint is the SHA1 hash of the file.
        # Otherwise, returns False
        current_catalog = self.load()
        # loop through all studies and files, looking for the file hash or source URL
        for study in current_catalog["studies"]:
            for study_files in study["files"]:
                if hash_string == study_files['fingerprint']:
                    return True
        return False
