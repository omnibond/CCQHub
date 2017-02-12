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

import os
import json
import traceback
import ConfigParser


global ccqHubConfigFileLocation

global ccqHubLookupDBName

global ccqHubObjectDBName

global ccqHubDBLock

global databaseType

global ccqHubPrefix


def init():
    global ccqHubConfigFileLocation
    global ccqHubDBLock
    global ccqHubLookupDBName
    global ccqHubObjectDBName
    global databaseType
    global ccqHubPrefix

    parser = ConfigParser.ConfigParser()

    # Check to see if the user has defined their own config file in their home directory
    if os.path.isfile("../ccqHub.conf"):
        ccqHubConfigFileLocation = "../ccqHub.conf"
        try:
            parser.read(ccqHubConfigFileLocation)
            try:
                databaseType = parser.get("Database", "databaseType")
                ccqHubLookupDBName = parser.get("Database", "lookupTableName")
                ccqHubObjectDBName = parser.get("Database", "objectTableName")
                ccqHubPrefix = parser.get("General", "ccqHubPrefix")
            except Exception as e:
                # There was an issue getting the database information out of the DB
                ccqHubLookupDBName = None
                ccqHubObjectDBName = None
                databaseType = "sqlite3"
                ccqHubPrefix = None

        except Exception as e:
            print traceback.format_exc(e)
            print "Unable to read ccqHub configuration file, the file may have been removed or corrupted."
            ccqHubLookupDBName = None
            ccqHubObjectDBName = None
            ccqHubPrefix = None

def initInstaller(prefix):
    global ccqHubConfigFileLocation
    global ccqHubDBLock
    global ccqHubLookupDBName
    global ccqHubObjectDBName
    global databaseType
    global ccqHubPrefix

    parser = ConfigParser.ConfigParser()

    # Check to see if the user has defined their own config file in their home directory
    if os.path.isfile(str(prefix) + "/ccqHub.conf"):
        ccqHubConfigFileLocation = str(prefix) + "/ccqHub.conf"
        try:
            parser.read(ccqHubConfigFileLocation)
            try:
                databaseType = parser.get("Database", "databaseType")
                ccqHubLookupDBName = parser.get("Database", "lookupTableName")
                ccqHubObjectDBName = parser.get("Database", "objectTableName")
                ccqHubPrefix = parser.get("General", "ccqHubPrefix")
            except Exception as e:
                # There was an issue getting the database information out of the DB
                ccqHubLookupDBName = None
                ccqHubObjectDBName = None
                databaseType = "sqlite3"
                ccqHubPrefix = None

        except Exception as e:
            print traceback.format_exc(e)
            print "Unable to read ccqHub configuration file, the file may have been removed or corrupted."
            ccqHubLookupDBName = None
            ccqHubObjectDBName = None
            ccqHubPrefix = None