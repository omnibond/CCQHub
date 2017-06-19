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

import json
import os
import sqlite3
import sys
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ccqHubVars
import compoundIndexDefinition
from DatabaseBaseClass import Database


class sqlLite3Database(Database):

    def returnDict(self, cursor, row):
        tempDict = {}
        for idx, col in enumerate(cursor.description):
            tempDict[col[0]] = row[idx]
        return tempDict

    def queryObj(self, limit, key, action, returnType, filter=None):
        returnArr = []
        if action == "query":
            tableConnInfo = self.tableConnect(ccqHubVars.ccqHubLookupDBName)
            if tableConnInfo['status'] != "success":
                print "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubLookupDBName)
                return {"status": "error", "payload": {"error": "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubLookupDBName), "traceback": traceback.format_stack()}}
            else:
                tableConn = tableConnInfo['payload']
                cursor = tableConn.cursor()
                data = None
                objectIds = []
                with ccqHubVars.ccqHubDBLock:
                    if filter is None:
                        searchKey = (str(key),)
                        cursor.execute("SELECT objectID FROM ccqHubLookup WHERE range_key=?", searchKey)
                        data = cursor.fetchall()
                    else:
                        # if there is a filter then query based on the filter
                        if filter == "beginsWith":
                            searchKey = (str(key)+"%",)
                            cursor.execute("SELECT objectID FROM ccqHubLookup WHERE range_key LIKE ?", searchKey)
                            data = cursor.fetchall()
                        elif filter == "greaterThan":
                            searchKey = (str(key),)
                            cursor.execute("SELECT objectID FROM ccqHubLookup WHERE range_key > ?", searchKey)
                            data = cursor.fetchall()
                        elif filter == "lessThan":
                            searchKey = (str(key),)
                            cursor.execute("SELECT objectID FROM ccqHubLookup WHERE range_key < ?", searchKey)
                            data = cursor.fetchall()
                        else:
                            tableConn.close()
                            return {"status": "error", "payload": {"error": "Database filter is not supported: " + str(filter), "traceback": traceback.format_stack()}}

                    # Close connection to lookup table
                    tableConn.close()
                    for item in data:
                        objectIds.append(item)

                    # Now we have a list of the objectIds that we want so we need to get the actual object out of the object table
                    tableConnInfo = self.tableConnect(ccqHubVars.ccqHubObjectDBName)
                    if tableConnInfo['status'] != "success":
                        print "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubObjectDBName)
                        return {"status": "error", "payload": {"error": "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubObjectDBName), "traceback": traceback.format_stack()}}
                    else:
                        tableConn = tableConnInfo['payload']
                        tableConn.row_factory = self.returnDict
                        nextCursor = tableConn.cursor()
                        for objectId in data:
                            try:
                                with ccqHubVars.ccqHubDBLock:
                                    nextCursor.execute("SELECT * FROM ccqHubObject WHERE hash_key=?", objectId)
                                    dbObject = nextCursor.fetchall()
                                # hash_key (string), meta_var (json object), job_script_text (string), sharingObj
                                item = {}
                                for itemData in dbObject:
                                    for thing in itemData:
                                        if thing == "meta_var":
                                            # Add each item in the meta_var_object to the return object to make it compatible with current ccq structure
                                            for key, value in json.loads(itemData['meta_var']).items():
                                                item[key] = value
                                        elif thing == "sharingObj":
                                            for key, value in json.loads(itemData['sharingObj']).items():
                                                item[key] = value
                                        elif thing == "hash_key":
                                            item['name'] = itemData[thing]
                                        else:
                                            item[thing] = itemData[thing]

                                returnArr.append(item)
                            except Exception as e:
                                print "Encountered an exception: " + str(traceback.format_exc(e))
                                pass
                        tableConn.close()

        elif action == "get":
            tableConnInfo = self.tableConnect(ccqHubVars.ccqHubObjectDBName)
            if tableConnInfo['status'] != "success":
                print "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubObjectDBName)
            else:
                tableConn = tableConnInfo['payload']
                tableConn.row_factory = self.returnDict
                cursor = tableConn.cursor()
                with ccqHubVars.ccqHubDBLock:
                    searchKey = (str(key),)
                    cursor.execute("SELECT * FROM ccqHubObject WHERE hash_key=?", searchKey)
                    dbObject = cursor.fetchall()
                    item = {}
                    for itemData in dbObject:
                        item['hash_key'] = itemData['hash_key']
                        meta_var = itemData['meta_var']
                        meta_var_object = json.loads(meta_var)
                        # Add each item in the meta_var_object to the return object to make it compatible with current ccq structure
                        for key, value in meta_var_object.items():
                            item[key] = value
                        item['jobScriptText'] = itemData['jobScriptText']
                        item['sharingObj'] = itemData['sharingObj']
                    returnArr.append(item)
                    tableConn.close()
        else:
            return {"status": "error", "payload": {"error": "Action not supported", "traceback": traceback.format_stack()}}

        return {"status": "success", "payload": returnArr}

    def handleObj(self, action, obj):
        baseObj = obj
        #print action
        #print type(obj)
        #Get a connection to both tables to be passed into the other functions
        lookupTableConnInfo = self.tableConnect(ccqHubVars.ccqHubLookupDBName)
        objectTableConnInfo = self.tableConnect(ccqHubVars.ccqHubObjectDBName)
        if lookupTableConnInfo['status'] != "success":
            print "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubLookupDBName)
            return {"status": "error", "payload": {"error": "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubLookupDBName), "traceback": traceback.format_stack()}}
        elif objectTableConnInfo['status'] != "success":
            print "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubObjectDBName)
            return {"status": "error", "payload": {"error": "Unable to successfully connect to the table " + str(ccqHubVars.ccqHubObjectDBName), "traceback": traceback.format_stack()}}
        else:
            lookupTableConn = lookupTableConnInfo['payload']
            objectTableConn = objectTableConnInfo['payload']

            if action == "delete":
                deleteIndexStatus = self.deleteIndexes(baseObj, lookupTableConn)
                if deleteIndexStatus['status'] == "success":
                    deleteObjectStatus = self.deleteObj(obj, objectTableConn)
                    if deleteObjectStatus['status'] == "success":
                        lookupTableConn.close()
                        objectTableConn.close()
                        return {"status": "success", "payload": "Successfully deleted the object"}
                    else:
                        lookupTableConn.close()
                        objectTableConn.close()
                        return {"status": "error", "payload": deleteObjectStatus['payload']}
                else:
                    lookupTableConn.close()
                    objectTableConn.close()
                    return {"status": "error", "payload": deleteIndexStatus['payload']}
            elif action == "create":
                addObjectStatus = self.addObj(obj, objectTableConn)
                if addObjectStatus['status'] == "success":
                    addIndexStatus = self.addIndexes(baseObj, lookupTableConn)
                    if addIndexStatus['status'] == "success":
                        lookupTableConn.close()
                        objectTableConn.close()
                        return {"status": "success", "payload": "Successfully added the object"}
                    else:
                        lookupTableConn.close()
                        objectTableConn.close()
                        return {"status": "error", "payload": addIndexStatus['payload']}
                else:
                    lookupTableConn.close()
                    objectTableConn.close()
                    return {"status": "error", "payload": addObjectStatus['payload']}
            elif action == "modify":
                deleteIndexStatus = self.deleteIndexes(baseObj, lookupTableConn)
                deleteObjectStatus = self.deleteObj(obj, objectTableConn)
                if deleteIndexStatus['status'] != "success":
                    print "Problem deleting the old object's indexes during the handleObj modify operation"
                    print deleteIndexStatus['payload']
                if deleteObjectStatus['status'] != "success":
                    print "Problem deleting the old object during the handleObj modify operation"
                    print deleteObjectStatus['payload']

                addObjectStatus = self.addObj(obj, objectTableConn)
                if addObjectStatus['status'] == "success":
                    addIndexStatus = self.addIndexes(baseObj, lookupTableConn)
                    if addIndexStatus['status'] == "success":
                        lookupTableConn.close()
                        objectTableConn.close()
                        return {"status": "success", "payload": "Successfully modified the object"}
                    else:
                        lookupTableConn.close()
                        objectTableConn.close()
                        print "Problem adding the new object's indexes during the handleObj modify operation"
                        return {"status": "error", "payload": addIndexStatus['payload']}
                else:
                    lookupTableConn.close()
                    objectTableConn.close()
                    print "Problem adding the new object during the handleObj modify operation"
                    return {"status": "error", "payload": addObjectStatus['payload']}
            else:
                lookupTableConn.close()
                objectTableConn.close()
                return {"status": "error", "payload": {"error": "Unsupported action passed to handleObj: " + str(action) + ". The supported actions are delete, create, and modify.", "traceback": traceback.format_stack()}}

    def addObj(self, obj, objectTableConnection):
        try:
            hash_key = obj['name']
        except KeyError as e:
            hash_key = obj['hash_key']
        try:
            jobScriptText = obj['jobScriptText']
        except KeyError as e:
            jobScriptText = ""

        try:
            sharingObject = obj['sharingObject']
        except KeyError:
            sharingObject = {}

        #Consolidate the metadata into a single object for storage in the DB, don't include the fields that have their own columns already
        meta_var = {}
        for x in obj:
            objType = type(obj[x])
            if "list" in str(objType) or "dict" in str(objType):
                #Don't put the sharingObject in the meta_var object as it has it's own column
                if str(x) != "sharingObject":
                    meta_var[x] = json.dumps(obj[x])
            elif str(x) != "jobScriptText" and str(x) != "name" and str(x) != "sharingObject":
                meta_var[x] = str(obj[x])
        try:
            with ccqHubVars.ccqHubDBLock:
                cursor = objectTableConnection.cursor()
                data = (str(hash_key), json.dumps(meta_var), str(jobScriptText), json.dumps(sharingObject))
                #print data
                #print "DATA"
                cursor.execute("INSERT INTO ccqHubObject VALUES (?, ?, ?, ?)", data)
                objectTableConnection.commit()
            return {"status": "success", "payload": "Successfully added  the object"}
        except Exception as e:
            print traceback.format_exc(e)
            return {"status": "error", "payload": {"error": "There was a problem trying to delete the object: " + str(hash_key), "traceback": traceback.format_exc(e)}}

    def deleteObj(self, obj, objectTableConnection):
        try:
            hash_key = obj['name']
        except KeyError as e:
            hash_key = obj['hash_key']
        try:
            with ccqHubVars.ccqHubDBLock:
                cursor = objectTableConnection.cursor()
                cursor.execute("DELETE FROM ccqHubObject WHERE hash_key=?", (hash_key,))
                objectTableConnection.commit()
        except Exception as e:
            print traceback.format_exc(e)
            return {"status": "error", "payload": {"error": "There was a problem trying to delete the object: " + str(hash_key), "traceback": traceback.format_exc(e)}}

        return {"status": "success", "payload": "Successfully deleted the object"}

    def addIndexes(self, obj, lookupTableConnection):
        RecType = obj['RecType']
        try:
            name = obj['name']
        except KeyError as e:
            name = obj['hash_key']
            obj['name'] = name

        indexValues = compoundIndexDefinition.returnCompoundIndexDefinition()

        cursor = lookupTableConnection.cursor()
        dataToInsert = []
        for x in range(len(indexValues)):
            tempIndex = indexValues[x]

            try:
                # Check to see if the index definition that we are on is for our RecType or not. If not just pass
                tempIndex[RecType]
                holderAttr = None
                try:
                    # Create the index from the definition found in compoundIndexDefinition
                    index = "RecType" + "-" + RecType + "-"
                    for y in range(len(tempIndex[RecType])):
                        attr = tempIndex[RecType][y]
                        try:
                            obj[attr] = json.loads(obj[attr])
                        except Exception as e:
                            pass
                        objType = type(obj[attr])
                        if "list" in str(objType) or "dict" in str(objType):
                            holderAttr = attr
                            if y < len(tempIndex[RecType]) - 1:
                                index += attr + "-holder-"
                            else:
                                index += attr + "-holder"
                        else:
                            if y < len(tempIndex[RecType]) - 1:
                                index += attr + "-" + str(obj[attr]) + "-"
                            else:
                                index += attr + "-" + str(obj[attr])

                    if "holder" in index and holderAttr is not None:
                        if len(obj[holderAttr]) == 0:
                            temp = index.replace("holder", "")
                        else:
                            for z in range(len(obj[holderAttr])):
                                temp = index.replace("holder", obj[holderAttr][z])
                        dataToInsert.append(('N/A', str(temp), str(name)))
                    else:
                        dataToInsert.append(('N/A', str(index), str(name)))
                except Exception as e:
                    print traceback.format_exc(e)
            except KeyError:
                pass
        try:
            if len(dataToInsert) > 0:
                with ccqHubVars.ccqHubDBLock:
                    cursor.executemany("INSERT INTO ccqHubLookup VALUES (?, ?, ?)", dataToInsert)
            lookupTableConnection.commit()
        except Exception as e:
            print traceback.format_exc(e)
            return {"status": "error", "payload": {"error": "Unable to save out the required indexes", "traceback": traceback.format_exc(e)}}

        return {"status": "success", "payload": "Successfully saved the indexes out"}

    def deleteIndexes(self, obj, lookupTableConnection):
        try:
            name = obj['name']
        except KeyError as e:
            name = obj['hash_key']
        try:
            with ccqHubVars.ccqHubDBLock:
                cursor = lookupTableConnection.cursor()
                cursor.execute("DELETE FROM ccqHubLookup WHERE objectID=?", (name,))
                lookupTableConnection.commit()
        except Exception as e:
            print traceback.format_exc(e)
            return {"status": "error", "payload": {"error": "There was a problem trying to delete the indexes for the object: " + str(name), "traceback": traceback.format_exc(e)}}

        return {"status": "success", "payload": "Successfully deleted the indexes for the object"}

    def createTable(self, tableName):
        try:
            with ccqHubVars.ccqHubDBLock:
                conn = sqlite3.connect(str(ccqHubVars.ccqHubPrefix) + "/var/" + str(tableName) + ".db")
                c = conn.cursor()
                if "lookup" in str(tableName).lower():
                    c.execute("CREATE TABLE ccqHubLookup (hash_key, range_key, objectID)")
                elif "object" in str(tableName).lower():
                    c.execute("CREATE TABLE ccqHubObject (hash_key, meta_var, jobScriptText, sharingObj)")
                else:
                    return {"status": "error", "payload": "Unsupported table name format"}
                conn.commit()
                conn.close()
            return {"status": "success", "payload": "Successfully created table " + str(tableName)}
        except Exception as e:
            return {"status": "error", "payload": {"error": "There was a problem trying to create the table.", "traceback": traceback.format_exc(e)}}

    def tableConnect(self, tableName):
        try:
            with ccqHubVars.ccqHubDBLock:
                conn = sqlite3.connect(str(ccqHubVars.ccqHubPrefix) + "/var/" + str(tableName) + ".db")
            return {"status": "success", "payload": conn}
        except Exception as e:
            print "There was a problem trying to connect to the local sqlite3 database: " + str(ccqHubVars.ccqHubPrefix) + "/" + str(tableName) + ".db"
            return {"status": "error", "payload": {"error": "There was a problem trying to connect to the local sqlite3 database", "traceback": traceback.format_exc(e)}}
