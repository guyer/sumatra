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
    
    JSON_PATTERN = "Sumatra.{}.json"

    def __init__(self, datreant_name="datreant://.smt/records"):
        if datreant_name.startswith("datreant://"):
            datreant_name = datreant_name[11:]
        self._datreant_name = datreant_name
        self.datreant = dtr.Group(self._datreant_name)
 
    def __str__(self):
        return "Record store using the datreant package (database file=%s)" % self.datreant.relpath

    def __getstate__(self):
        return {'datreant_name': self._datreant_name}

    def __setstate__(self, state):
        self.__init__(**state)

    def list_projects(self):
        return self.datreant.members.names

    def has_project(self, project_name):
        return project_name in self.datreant.members.names
        
    def _records(self, project_name):
        return self.datreant.members[project_name][0].members
        
    def save(self, project_name, record):
        if self.has_project(project_name):
            records = self._records(project_name)
        else:
            records = dtr.Group(os.path.join(self.datreant.relpath,
                                             project_name))
            self.datreant.members.add(records)
            records = records.members
            
        treant = dtr.Treant(treant=os.path.join(record.datastore.root,
                                                record.label))
        treant.tags = record.tags
        path = treant[self.JSON_PATTERN.format(record.label)].make().abspath
        with open(path, 'w') as f:
            f.write(serialization.encode_record(record))
        
        records.add(treant)

    def _treants2records(self, treants):
        jsons = treants.glob(self.JSON_PATTERN.format("*"))
        return jsons.map(lambda leaf: serialization.decode_record(leaf.read()))

    def get(self, project_name, label):
        return self._treants2records(self._records(project_name)[label])[0]

    def list(self, project_name, tags=None):
        if self.has_project(project_name):
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
        else:
            records = []
        return records

    def labels(self, project_name):
        if self.has_project(project_name):
            return self._records(project_name).names
        else:
            return []

    def delete(self, project_name, label):
        records = self._records(project_name)
        if label in records.names:
            self._records(project_name).remove(label)
        else:
            # datreant doesn't care, but sumatra wants an error
            # for a non-existent label
            raise KeyError

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

    def clear(self):
        for project in self.datreant.members:
            for record in project.members:
                record._backend.delete()
                project.members.remove(record)
            project._backend.delete()
            self.datreant.members.remove(project)
        self.datreant._backend.delete()
        self.datreant = None

    @classmethod
    def accepts_uri(cls, uri):
        return uri[:11] == "datreant://"

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
