"""
Handles storage of simulation/analysis records on a remote server using HTTP.

The server should support the following URL structure and HTTP methods:

/                                            GET
/<project_name>/[?tags=<tag1>,<tag2>,...]    GET
/<project_name>/tag/<tag>/                   GET, DELETE
/<project_name>/<record_label>/              GET, PUT, DELETE

and should both accept and return JSON-encoded data when the Accept header is
"application/json".

The required JSON structure can be seen in recordstore.serialization.


:copyright: Copyright 2006-2014 by the Sumatra team, see doc/authors.txt
:license: CeCILL, see LICENSE for details.
"""

from warnings import warn
from urlparse import urlparse, urlunparse
try:
    import httplib2
    have_http = True
except ImportError:
    have_http = False
from sumatra.recordstore.base import RecordStore, RecordStoreAccessError
from sumatra.recordstore import serialization
from ..core import registry
import json
import mimetypes
from StringIO import StringIO

API_VERSION = 3


def domain(url):
    return urlparse(url).netloc


def process_url(url):
    """Don't change anything to the url"""
    return url


class DDSMRecordStore(RecordStore):
    """
    Handles storage of simulation/analysis records on a remote server using HTTP.

    The server should support the following URL structure and HTTP methods:

    =========================================                              ================
    /api/v1/private/<api_token>/project/pull/<project_name>                 GET
    /api/v1/private/<api_token>/project/push/<project_name>                 POST
    /api/v1/private/<api_token>/project/sync/<project_name>                 PUT
    /api/v1/private/<api_token>/record/display/<project_name>/<record_id>   GET
    /api/v1/private/<api_token>/record/pull/<project_name>                  GET
    /api/v1/private/<api_token>/record/push/<project_name>                  POST
    /api/v1/private/<api_token>/record/sync/<project_name>/<record_id>      PUT
    =========================================                              ================

    """

    def __init__(self, server_url):
        self.server_url = process_url(server_url)
        if self.server_url[-1] != "/":
            self.server_url += "/"
        self.client = httplib2.Http('.cache')

    def __str__(self):
        return "Interface to remote record store at %s using HTTP" % self.server_url

    def __getstate__(self):
        return {
            'server_url': self.server_url
        }

    def __setstate__(self, state):
        self.__init__(state['server_url'])

    def _get(self, url):
        headers = {'Accept': 'application/json'}
        print "_get Url: %s"%url
        response, content = self.client.request(url, headers=headers)
        return response, content

    def list_projects(self):
        response, content = self._get(self.server_url+'project/pull')
        if response.status != 200:
            raise RecordStoreAccessError("Error in accessing %s\n%s: %s" % (self.server_url, response.status, content))
        return [entry['id'] for entry in serialization.decode_project_list(content)]

    def create_project(self, project_name, long_name='', description='', goals=''):
        url = "%sproject/push/%s" % (self.server_url, project_name)
        data = serialization.encode_project_info(long_name, description=description, goals=goals)
        headers = {'Content-Type': 'application/json'}
        response, content = self.client.request(url, 'POST', data, headers=headers)
        if response.status != 201:
            raise RecordStoreAccessError("%d\n%s" % (response.status, content))

    def update_project_info(self, project_name, long_name='', description=''):
        url = "%sproject/sync/%s" % (self.server_url, project_name)
        data = serialization.encode_project_info(long_name, description)
        headers = {'Content-Type': 'application/json'}
        response, content = self.client.request(url, 'PUT', data, headers=headers)
        if response.status != 200:
            raise RecordStoreAccessError("%d\n%s" % (response.status, content))

    def encode_multipart_formdata(self, fields, files):
        """
        fields is a sequence of (name, value) elements for regular form fields.
        files is a sequence of (name, filename, value) elements for data to be uploaded as files
        Return (content_type, body) ready for httplib.HTTP instance
        """
        BOUNDARY = '----------lImIt_of_THE_fIle_eW_$'
        CRLF = '\r\n'
        L = []
        for (key, value) in fields:
            L.append('--' + BOUNDARY)
            L.append('Content-Disposition: form-data; name="%s"' % key)
            L.append('')
            L.append(value)
        for (key, filename, value) in files:
            L.append('--' + BOUNDARY)
            L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
            L.append('Content-Type: %s' % self.get_content_type(filename))
            L.append('')
            L.append(value)
        L.append('--' + BOUNDARY + '--')
        L.append('')
        body = CRLF.join(L)
        content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
        return content_type, body

    def get_content_type(self, filename):
        return mimetypes.guess_type(filename)[0] or 'application/octet-stream'

    def update_project_container(self, project_name, container):
        url = "%sproject/sync/%s" % (self.server_url, project_name)
        data = {}
        container_parts = container.split(":")
        local = ''
        if len(container_parts) == 1:
            data['image'] = {}
            if "http://" in container_parts[0]:
                data['image']['scope'] = container_parts[0]
            else:
                data['image']['scope'] = 'local'
                local = container_parts[0]
        elif len(container_parts) == 2:
            data['system'] = container_parts[0]
            data['image'] = {}
            if "http://" in container_parts[1]:
                data['image']['scope'] = container_parts[1]
            else:
                data['image']['scope'] = 'local'
                local = container_parts[1]

        if len(container_parts) >= 1:

            print "Image name is: %s" %local

            files = []
            files.append(("data", "request.json", json.dumps(data)))
            if local != '':
                ref_buffer = StringIO()
                with open(local, 'rb') as fh:
                    ref_buffer.write(fh.read())
                ref_buffer.seek(0)

                # print ref_buffer.read()
                # ref_buffer.seek(0)

                # print "%s"%ref_buffer.read()
                files.append(("image", local, ref_buffer.read().decode("ascii", "replace")))

            content_type, body = self.encode_multipart_formdata([], files)
            headers = {'Content-Type': content_type}
            response, content = self.client.request(url, 'POST', body.encode("ascii", "replace"), headers=headers)
            if response.status != 201:
                raise RecordStoreAccessError("%d\n%s" % (response.status, content))


    def has_project(self, project_name):
        url = "%sproject/pull/%s" % (self.server_url, project_name)
        response, content = self._get(url)
        if response.status == 200:
            return True
        elif response.status in (401, 404):
            return False
        else:
            raise RecordStoreAccessError("%d\n%s" % (response.status, content))

    def project_info(self, project_name):
        """Return a project's long name and description."""
        url = "%sproject/pull/%s" % (self.server_url, project_name)
        response, content = self._get(url)
        if response.status != 200:
            raise RecordStoreAccessError("Error in accessing %s\n%s: %s" % (url, response.status, content))
        data = serialization.decode_project_data(content)
        return dict((k, data[k]) for k in ("name", "description"))

    def save(self, project_name, record, status="unknown"):
        data = serialization.encode_record(record)
        json_data = json.loads(data)
        if not self.has_project(project_name):
            self.create_project(project_name, description=json_data["outcome"], goals=json["reason"])

        if status != "unknown":
            url = "%srecord/sync/%s/%s" % (self.server_url, project_name, record.label)
            headers = {'Content-Type': 'application/json'}
            # data = serialization.encode_record(record)
            # json_data = json.loads(data)
            ddsm_record = {}
            ddsm_record["status"] = status
            ddsm_record["system"] = json_data["platforms"][0] if len(json_data["platforms"]) > 0 else []
            del json_data["platforms"]
            ddsm_record["program"] = {"executable":json_data["executable"], "repository":json_data["repository"], "main_file":json_data["main_file"], "launch_mode":json_data["launch_mode"], "version":json_data["version"]}
            del json_data["executable"]
            del json_data["repository"]
            del json_data["main_file"]
            del json_data["launch_mode"]
            del json_data["version"]
            ddsm_record["inputs"] = json_data["input_data"]
            # json_data["parameters"]["type"] = "parameters"
            ddsm_record["inputs"].append({"parameters":json_data["parameters"]})
            # json_data["script_arguments"]["type"] = "script_arguments"
            ddsm_record["inputs"].append({"script_arguments":json_data["script_arguments"]})
            del json_data["input_data"]
            del json_data["parameters"]
            del json_data["script_arguments"]
            ddsm_record["outputs"] = json_data["output_data"]
            # json_data["stdout_stderr"]["type"] = "stdout_stderr"
            ddsm_record["outputs"].append({"stdout_stderr":json_data["stdout_stderr"]})
            del json_data["output_data"]
            del json_data["stdout_stderr"]
            ddsm_record["dependencies"] = json_data["dependencies"]
            del json_data["dependencies"]

            ddsm_record["data"] = json_data

            print json.dumps(ddsm_record)

            response, content = self.client.request(url, 'PUT', json.dumps(ddsm_record),
                                                headers=headers)
        else:
            url = "%srecord/push/%s" % (self.server_url, project_name)
            headers = {'Content-Type': 'application/json'}
            data = serialization.encode_record(record)
            json_data = json.loads(data)
            ddsm_record = {}
            ddsm_record["status"] = status
            ddsm_record["system"] = json_data["platforms"][0] if len(json_data["platforms"]) > 0 else {}
            del json_data["platforms"]
            ddsm_record["program"] = {"executable":json_data["executable"], "repository":json_data["repository"], "main_file":json_data["main_file"], "launch_mode":json_data["launch_mode"], "version":json_data["version"]}
            del json_data["executable"]
            del json_data["repository"]
            del json_data["main_file"]
            del json_data["launch_mode"]
            del json_data["version"]
            ddsm_record["inputs"] = json_data["input_data"]
            # json_data["parameters"]["type"] = "parameters"
            ddsm_record["inputs"].append({"parameters":json_data["parameters"]})
            # json_data["script_arguments"]["type"] = "script_arguments"
            ddsm_record["inputs"].append({"script_arguments":json_data["script_arguments"]})
            del json_data["input_data"]
            del json_data["parameters"]
            del json_data["script_arguments"]
            ddsm_record["outputs"] = json_data["output_data"]
            # json_data["stdout_stderr"]["type"] = "stdout_stderr"
            ddsm_record["outputs"].append({"stdout_stderr":json_data["stdout_stderr"]})
            del json_data["output_data"]
            del json_data["stdout_stderr"]
            ddsm_record["dependencies"] = json_data["dependencies"]
            del json_data["dependencies"]

            ddsm_record["data"] = json_data

            print json.dumps(ddsm_record)

            response, content = self.client.request(url, 'POST', json.dumps(ddsm_record),
                                                headers=headers)
            record.label = content
            print "Record label: %s"%content
        if response.status not in (200, 201):
            raise RecordStoreAccessError("%d\n%s" % (response.status, content))

    def _get_record(self, project_name, record):
        url = "%srecord/display/%s/%s" % (self.server_url, project_name, record.label)
        response, content = self._get(url)
        if response.status != 200:
            if response.status == 404:
                raise KeyError("No record was found at %s" % url)
            else:
                raise RecordStoreAccessError("%d\n%s" % (response.status, content))
        return serialization.decode_record(content)

    def get(self, project_name, label):
        return self._get_record(project_name, label)

    def list(self, project_name, tags=None):
        url = "%srecord/pull/%s" % (self.server_url, project_name)
        response, content = self._get(url)
        if response.status != 200:
            raise RecordStoreAccessError("Could not access %s\n%s: %s" % (url, response.status, content))
        record_urls = serialization.decode_project_data(content)["records"]
        records = []
        for record_url in record_urls:
            records.append(self._get_record(record_url))
        return records

    def labels(self, project_name):
        # return [record.label for record in self.list(project_name)]  # probably inefficient
        return ""

    def delete(self, project_name, label):
        # url = "%s%s/%s/" % (self.server_url, project_name, label)
        # response, deleted_content = self.client.request(url, 'DELETE')
        # if response.status != 204:
        #     raise RecordStoreAccessError("%d\n%s" % (response.status, deleted_content))
        warn("Cannot delete from a ddsm record store directly. Contact the record store administrator")

    def delete_by_tag(self, project_name, tag):
        # url = "%s%s/tag/%s/" % (self.server_url, project_name, tag)
        # response, n_records = self.client.request(url, 'DELETE')
        # if response.status != 200:
        #     raise RecordStoreAccessError("%d\n%s" % (response.status, n_records))
        # return int(n_records)
        return 0

    def most_recent(self, project_name):
        # url = "%s%s/last/" % (self.server_url, project_name)
        # return self._get_record(url).label
        return ""

    def sync(self, other, project_name):
        if not self.has_project(project_name):
            self.create_project(project_name)
        super(DDSMRecordStore, self).sync(other, project_name)

    def clear(self):
        warn("Cannot clear a ddsm record store directly. Contact the record store administrator")

    @classmethod
    def accepts_uri(cls, uri):
        return (uri[:4] == "http") and ("api/v1/private" in uri)
        # uri[:4] == "http"


if have_http:
    registry.register(DDSMRecordStore)
