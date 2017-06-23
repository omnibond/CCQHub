#!/usr/bin/python2.7
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

global ccqHubFileLock

global ccqHubVarLock

global databaseType

global ccqHubPrefix

global jobMappings
# {"<job_id>": {<job_info>}}

global jobsTransferringOutput
# [ <job1>, <job2>,]

global ccqHubVarFileBackup


def init():
    global ccqHubConfigFileLocation
    global ccqHubDBLock
    global ccqHubFileLock
    global ccqHubVarLock
    global ccqHubLookupDBName
    global ccqHubObjectDBName
    global databaseType
    global ccqHubPrefix
    global jobMappings
    global ccqHubVarFileBackup
    global jobsTransferringOutput

    parser = ConfigParser.ConfigParser()

    # Check to see if the user has defined their own config file in their home directory
    if os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + "/../etc/ccqHub.conf"):
        ccqHubConfigFileLocation = os.path.dirname(os.path.realpath(__file__)) + "/../etc/ccqHub.conf"
        try:
            parser.read(ccqHubConfigFileLocation)
            try:
                databaseType = parser.get("Database", "databaseType")
                ccqHubLookupDBName = parser.get("Database", "lookupTableName")
                ccqHubObjectDBName = parser.get("Database", "objectTableName")
                ccqHubPrefix = parser.get("General", "ccqHubPrefix")
                ccqHubDBLock = None
                ccqHubFileLock = None
                ccqHubVarLock = None
                jobsTransferringOutput = []
                ccqHubVarFileBackup = str(ccqHubPrefix) + "/var/ccqHubVarsBackup.json"
                if os.path.isfile(ccqHubVarFileBackup):
                    try:
                        with open(ccqHubVarFileBackup, "r") as ccqFile:
                            backedUpObjects = json.load(ccqFile)
                            jobMappings = backedUpObjects['jobMappings']
                    except Exception as e:
                        print "There was no backup file found, it was either deleted or this is the first time ccqHub has been run."
                        jobMappings = {}
                else:
                    jobMappings = {}
            except Exception as e:
                print "ERROR" + str(e)
                # There was an issue getting the database information out of the DB
                ccqHubLookupDBName = None
                ccqHubObjectDBName = None
                databaseType = None
                ccqHubPrefix = None
                ccqHubDBLock = None
                ccqHubFileLock = None
                ccqHubVarLock = None
                jobMappings = None
                ccqHubVarFileBackup = None
                jobsTransferringOutput = []

        except Exception as e:
            print traceback.format_exc(e)
            print "Unable to read ccqHub configuration file, the file may have been removed or corrupted."
            ccqHubLookupDBName = None
            ccqHubObjectDBName = None
            ccqHubPrefix = None
            ccqHubDBLock = None
            ccqHubFileLock = None
            ccqHubVarLock = None
            databaseType = None
            jobMappings = None
            ccqHubVarFileBackup = None
            jobsTransferringOutput = []

    else:
        ccqHubLookupDBName = None
        ccqHubObjectDBName = None
        ccqHubPrefix = None
        ccqHubDBLock = None
        ccqHubFileLock = None
        ccqHubVarLock = None
        databaseType = None
        jobMappings = None
        ccqHubVarFileBackup = None
        jobsTransferringOutput = []

def initInstaller(prefix):
    global ccqHubConfigFileLocation
    global ccqHubDBLock
    global ccqHubFileLock
    global ccqHubVarLock
    global ccqHubLookupDBName
    global ccqHubObjectDBName
    global databaseType
    global ccqHubPrefix
    global ccqHubVarFileBackup
    global jobsTransferringOutput

    parser = ConfigParser.ConfigParser()

    # Check to see if the user has defined their own config file in their home directory
    if os.path.isfile(str(prefix) + "/etc/ccqHub.conf"):
        ccqHubConfigFileLocation = str(prefix) + "/etc/ccqHub.conf"
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
                databaseType = None
                ccqHubPrefix = None
                jobsTransferringOutput = []

        except Exception as e:
            print traceback.format_exc(e)
            print "Unable to read ccqHub configuration file, the file may have been removed or corrupted."
            ccqHubLookupDBName = None
            ccqHubObjectDBName = None
            ccqHubPrefix = None
            jobsTransferringOutput = []


#This function takes in a section name and a key name and then returns the value of that key that is in the config file.
def retrieveSpecificConfigFileKey(section, key):
    parser = ConfigParser.ConfigParser()
    if os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + "/../etc/ccqHub.conf"):
        try:
            parser.read(ccqHubConfigFileLocation)
            try:
                requestedKeyValue = parser.get(str(section), str(key))
                return {"status": "success", "payload": requestedKeyValue}
            except Exception as e:
                return {"status": "error", "payload": "There was an error encountered trying to obtain the key from the config file.\n" + ''.join(traceback.format_exc(e))}
        except Exception as e:
            return {"status": "error", "payload": "Unable to read ccqHub configuration file, the file does not exist.\n" + ''.join(traceback.format_exc(e))}
    else:
        return {"status": "error", "payload": "Unable to read ccqHub configuration file, the file does not exist."}
