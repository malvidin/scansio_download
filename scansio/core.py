"""
Interacts with the Scans.io catalog, the files it links to, and a local JSON catalog of previously downloaded files
The import of the elastic search module only occurs if the local catalog is 'elasticsearch'
"""


import os
import requests
import json
import hashlib
import logging


def download_large_file(download_link, target_file):
    logging.info('downloading %s to %s', download_link, target_file)
    file_download = requests.get(download_link, stream=True)
    with open(target_file, 'wb') as file_target:
        for chunk in file_download.iter_content(chunk_size=1024):
            if chunk:
                file_target.write(chunk)


def generate_sha1_hash(target_file):
    logging.info('generating hash for %s', target_file)
    # calculate the sha1 hash of a file
    with open(target_file, 'rb') as file_target:
        h = hashlib.sha1()
        while True:
            data = file_target.read(8192)
            if not data:
                break
            h.update(data)
    sha1 = h.hexdigest()
    logging.info('sha1 hash: %s', sha1)
    return sha1


class Download:
    """ Download files from scans.io """

    def __init__(self, catalog_url="https://scans.io/json"):
        # The local catalog is currently a JSON file with similar structure to the Scans.io JSON catalog
        # Should be able to validate against Elastic Search in the future"
        self.catalog_url = catalog_url

    def download_catalog(self):
        # Download catalog from remote URL
        # returns loaded JSON of catalog, or False
        catalog_response = requests.get(self.catalog_url)
        if catalog_response.status_code == 200:
            catalog_json = json.loads(catalog_response.text)
            logging.info('file downloaded successfully from %s', self.catalog_url)
            return catalog_json
        else:
            logging.error('download failure of %s with status %d', self.catalog_url, catalog_response.status_code)
            return False

    def download_latest_study(self, study_id, catalog_class, file_filter=""):
        # Keep same name as before
        result = self.download_study_files(study_id, catalog_class, -1, file_filter)
        if type(result) == list:
            return result[0]
        else:
            return result

    def download_study_files(self, study_id, catalog_class, count=0, url_filter=""):
        # downloads all files from a scans.io study, using the uniqid from https://scans.io/json
        # Returns list of downloaded file names, or False if non-download errors occur
        # Use count to limit the returned values.
        # Positive integers retrieve the oldest X studies, Negative integers retrieve newest X studies
        assert type(count) == int
        study_found = False
        downloaded_file_list = []
        current_catalog = self.download_catalog()
        if current_catalog is not False:
            # Get URLs and h*ashes from current catalog
            for study in current_catalog["studies"]:
                if study["uniqid"] == study_id:
                    logging.info('Found study ID')
                    study_found = True
                    # Sort the study, oldest to newest.
                    study_file_list = study.pop("files")
                    # apply URL filter before counting the matching results
                    study_file_list[:] = [f for f in study_file_list if url_filter in f.get("name")]
                    study_metadata = study
                    study_file_list.sort(key=self.date_to_int, reverse=False)
                    logging.info('found %d files after applying filter', len(study_file_list))
                    if len(study_file_list) == 0:
                        return False
                    elif len(study_file_list) < abs(count) or count == 0:
                        pass
                    elif count > 0:
                        del study_file_list[count:]
                    elif count < 0:
                        del study_file_list[:count]
                    for study_file_info in study_file_list:
                        study_filename = study_file_info["name"].split("/")[-1]
                        # Check if already downloaded
                        if catalog_class.contains(study_file_info["fingerprint"]):
                            pass
                        # If not, download and add to local catalog
                        else:
                            # Download file
                            download_large_file(study_file_info["name"], study_filename)
                            # verify downloaded file
                            observed_hash = generate_sha1_hash(study_filename)
                            if observed_hash == study_file_info["fingerprint"]:
                                # Hash verified; add verification to the local catalog
                                study_file_info["verified"] = True
                                logging.info("%s hash verified as %s", study_filename, observed_hash)
                                downloaded_file_list.append(study_filename)
                            else:
                                logging.info("hash verification failed, expected %s, observed %s",
                                            study_file_info["fingerprint"], observed_hash)
                                return downloaded_file_list  # Fail early if downloaded file doesn't verify
                            # Write to catalog after validation
                            logging.info('sending to parser and catalog')
                            write_success = catalog_class.write(study_metadata, study_file_info)
                            if write_success is False:
                                return False
            if not study_found:
                logging.error("study not found")
                return False
            else:
                logging.info('returning list of downloaded files')
                return downloaded_file_list

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

    def __init__(self, local_catalog_file="local_catalog.json", parser=lambda x: True):
        self.local_catalog_file = local_catalog_file
        self.parser = parser

    def load(self):
        # returns loaded JSON of catalog, or False
        if os.path.isfile(self.local_catalog_file):
            with open(self.local_catalog_file, "r") as catalog_file:
                local_catalog_json = json.load(catalog_file)
        else:
            logging.info('file not found, creating local catalog')
            local_catalog_json = {"studies": []}
        return local_catalog_json

    def write(self, study, study_file_info):
        # study is the study metadata, minus the file objects
        # study_file_info is one member of the file information list
        study_id = study['uniqid']
        study_filename = study_file_info['name'].split("/")[-1]
        local_catalog = self.load()
        study_found = False
        logging.info('starting write to catalog')
        if self.parser(study_filename) == True:
            logging.info('parser success (if defined)')
            for study in local_catalog['studies']:
                if study['uniqid'] == study_id:
                    study_found = True
                    try:
                        study['files'].append(study_file_info)
                    except KeyError:
                        study['files'] = [study_file_info]
            if not study_found:
                study['files'] = [study_file_info]
                local_catalog['studies'].append(study)
            logging.info('writing catalog')
            json.dump(local_catalog, open(self.local_catalog_file, 'w'))
            return True
        else:
            logging.error("parser failed, not writing to catalog")
            return False

    def contains(self, study_hash):
        # Returns True if 'name' or 'fingerprint' of a study is in the local catalog
        # The 'name' is the source URL of the study, the fingerprint is the SHA1 hash of the file.
        # Otherwise, returns False
        current_catalog = self.load()
        # loop through all studies and files, looking for the file hash or source URL
        for study in current_catalog["studies"]:
            for study_files in study["files"]:
                if study_hash == study_files['fingerprint']:
                    logging.info("%s already exists in local catalog", study_hash)
                    return True
        logging.info("%s not found in local catalog", study_hash)
        return False
