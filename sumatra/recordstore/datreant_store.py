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

import datreant.core as dtr
from sumatra.recordstore.base import RecordStore
from sumatra.formatting import record2dict
from sumatra.recordstore import serialization
from ..core import component


@component
class DatreantRecordStore(RecordStore):
    """
    Handles storage of simulation/analysis records based on the Python 
    :mod:`datreant` package.

    The advantage of this record store is that it has no dependencies. The
    disadvantages are that it allows only local access and does not support
    the *smtweb* interface.
    """

    def __init__(self, datreant_name="__ignored__"):
        self.datreant = dtr.Group(".smt/records")
 
    def __str__(self):
        return "Record store using the datreant package (database file=%s)" % self.datreant.relpath

    def __getstate__(self):
        return {'datreant_name': self.datreant.relpath}

    def __setstate__(self, state):
        self.__init__(**state)

    def list_projects(self):
        return self.datreant.members.names

    def has_project(self, project_name):
        return project_name in self.datreant.members.names
        
    def _records(self, project_name):
        return self.datreant.members[project_name].members
        
    def save(self, project_name, record):
        if self.has_project(project_name):
            records = self._records[project_name]
        else:
            records = dtr.Group(os.path.join(self.datreant.relpath, project_name))
            self.datreant.members.add(records)
            records = records.members
            
        treant = dtr.Treant(treant=os.path.join(record.label))
        with open(treant["{}.sumatra.json".format(record.label)].make().abspath, 'w') as f:
            f.write(serialization.encode_record(record))
        
        records.add(treant)

    def get(self, project_name, label):
        return self._records(project_name)[label]

    def list(self, project_name, tags=None):
        if self.has_project(project_name):
            records = self._records(project_name)
            if tags is not None:
                # we need a tuple for "or" semantics in datreant
                try:
                    tags = tuple(tags)
                except TypeError:
                    tags = (tags,)

                records = records[records.tags[tags]]
            records = [build_record(r) for r in records]
        else:
            records = []
        return records

    def labels(self, project_name):
        if self.has_project(project_name):
            return self._records(project_name).names
        else:
            return []

    def delete(self, project_name, label):
        self._records(project_name).remove(label)

    def delete_by_tag(self, project_name, tag):
        records = self._records(project_name)
        for_deletion = records[records.tags[tag]].names
        for record in for_deletion:
            self.delete(project_name, record)
        return len(for_deletion)

    def most_recent(self, project_name):
        most_recent = None
        most_recent_timestamp = datetime.min
        for record in self._records(project_name):
            if record.timestamp > most_recent_timestamp:
                most_recent_timestamp = record.timestamp
                most_recent = record.label
        return most_recent

    @classmethod
    def accepts_uri(cls, uri):
        return uri == "@datreant@"

    def backup(self):
        """
        Copy the database file
        """
        shutil.copy2(self.datreant.relpath, self.datreant.relpath + ".backup")

    def remove(self):
        """
        Delete the database entirely.
        """
        self.backup()
        os.remove(self.datreant.relpath)
