#Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of CCQHub.

# CCQHub is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# CCQHub is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with CCQHub.  If not, see <http://www.gnu.org/licenses/>.
import uuid


class Database:

    def __init__(self):
        pass

    def generateTableNames(self):
        ccqHubUuid = str(uuid.uuid4().get_hex().upper()[0:6])
        lookupTableName = "ccqHubLookupTable-" + ccqHubUuid
        objectTableName = "ccqHubObjectTable-" + ccqHubUuid
        return {"status": "success", "payload": {"lookupTableName": str(lookupTableName), "objectTableName": str(objectTableName)}}

    # Base classes
    def queryObj(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: queryObj"}

    def handleObj(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: handleObj"}

    def addObj(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: addObj"}

    def deleteObj(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: deleteObj"}

    def addIndexes(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: addIndexes"}

    def deleteIndexes(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: deleteIndexes"}

    def createTable(self, **kwargs):
        # Schema for the Lookup table must only contain the following fields and types:
        # hash_key (string), range_key (string), objectID (string)

        # Schema for the Object Table must only contain the following fields and types:
        # hash_key (string), meta_var (json object), jobScriptText (string), sharingObj (json object)

        return {"status": "error", "payload": "Base Database Class Not Called Error In: createTable"}

    def tableConnect(self, **kwargs):
        return {"status": "error", "payload": "Base Database Class Not Called Error In: tableConnect"}