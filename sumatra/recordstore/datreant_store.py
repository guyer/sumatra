"""
Handles storage of simulation/analysis records based on the Python datreant package.

:copyright: Copyright 2006-2017 by the Sumatra team, see doc/authors.txt
:license: BSD 2-clause, see LICENSE for details.
"""
from __future__ import unicode_literals
from builtins import str

import os
import shutil
from datetime import datetime

import datreant as dtr
from sumatra.recordstore.base import RecordStore
from sumatra.formatting import record2dict
from sumatra.recordstore import serialization
from ..core import component

# __all__ = ['DatreantRecordStore']

JSON_PATTERN = "Sumatra.{}.json"
DTR_PROTOCOL = "datreant://"

@component
class DatreantRecordStore(RecordStore):
    """
    Handles storage of simulation/analysis records based on the Python
    :mod:`datreant` package.

    The advantage of this record store is that it is daemonless, has only
    lightweight dependencies, and the records are stored in human-readable JSON,
    maximizing portability. This store is also immune to the security and
    long-term storage vulnerabilities of shelve/pickle.

    The disadvantages are that it allows only local access, it does not support
    the *smtweb* interface, and access can be slow compared to a centralized
    database.
    """
    
    def __init__(self, datreant_name=DTR_PROTOCOL + "Data/"):
        if datreant_name.startswith(DTR_PROTOCOL):
            datreant_name = datreant_name[len(DTR_PROTOCOL):]
        self._datreant_name = os.path.join("/tmp", datreant_name)
        self.datreant = dtr.Treant(self._datreant_name)
 
    def __str__(self):
        return "Record store using the datreant package (database file=%s)" % self._datreant_name

    def __getstate__(self):
        return {'datreant_name': self._datreant_name}

    def __setstate__(self, state):
        self.__init__(**state)

    def list_projects(self):
        records = dtr.discover(self.datreant)
        projects = list(set(records.categories['smt_project']))
        projects.remove(None)

        return projects

    def has_project(self, project_name):
        return len(self._records(project_name)) > 0

    def _records(self, project_name):
        records = dtr.Bundle(self.datreant.children().abspaths)
        mask = [c == project_name for c in records.categories['smt_project']]
        return records[mask]
        
    def save(self, project_name, record):
        treant = dtr.Treant(treant=self.datreant[record.label].abspath)
        treant.categories['smt_project'] = project_name

        treant.tags = record.tags

        jsonpath = treant[JSON_PATTERN.format(record.label)].make().abspath
        with open(jsonpath, 'w') as f:
            f.write(serialization.encode_record(record))

    def _treants2records(self, treants):
        jsons = treants.glob(JSON_PATTERN.format("*"))
        return jsons.map(lambda leaf: serialization.decode_record(leaf.read()))

    def get(self, project_name, label):
        treant = self.datreant[label]
        if treant.exists:
            treant = dtr.Treant(treant)
        if (not treant.exists
            or treant.categories['smt_project'] != project_name):
            errstr = "Record {} is not in project {}".format(label,
                                                             project_name)
            raise IndexError(errstr)
        return self._treants2records(treant)[0]

    def list(self, project_name, tags=None):
        records = self._records(project_name)
        if tags is not None:
            # we need a tuple for "or" semantics in datreant
            if not isinstance(tags, tuple):
                if isinstance(tags, list):
                    tags = tuple(tags)
                else:
                    tags = (tags,)

            if len(tags) > 0:
                # `smt list` passes an empty list to mean all tags
                # but datreant takes that to mean nothing matches
                records = records[records.tags[tags]]
        records = self._treants2records(records)

        return records

    def labels(self, project_name, tags=None):
        records = self._records(project_name)
        if tags:
            if not isinstance(tags, (tuple, list)):
                tags = (tags,)
            lbls = records[records.tags[tuple(tags)]].names
        else:
            lbls = records.names

        return lbls

    def delete(self, project_name, label):
        record = self.datreant[label]
        if not record.exists:
            # datreant doesn't care, but sumatra wants an error
            # for a non-existent label
            raise KeyError

        record = dtr.Treant(record)
        if record.categories['smt_project'] != project_name:
            # This will never fail, as we only put one project in the
            # Data/ directory, but fail if the project_name is wrong
            raise KeyError

        shutil.rmtree(record._treantdir)

    def delete_by_tag(self, project_name, tag):
        records = self._records(project_name)
        for_deletion = records[records.tags[tag]].names
        for record in for_deletion:
            self.delete(project_name, record)
        return len(for_deletion)

    def most_recent(self, project_name):
        most_recent = None
        most_recent_timestamp = datetime.min
        for record in self._treants2records(self._records(project_name)):
            if record.timestamp > most_recent_timestamp:
                most_recent_timestamp = record.timestamp
                most_recent = record.label
        return most_recent

    @classmethod
    def accepts_uri(cls, uri):
        return uri.startswith(DTR_PROTOCOL)

    def backup(self):
        """
        Copy the database file
        """
        shutil.copytree(self._datreant_name, self._datreant_name + ".backup")

    def remove(self):
        """
        Delete the database entirely.
        """
        self.backup()
        shutil.rmtree(self._datreant_name)
