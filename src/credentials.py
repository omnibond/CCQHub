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
import sys
import policies
import ccqHubMethods
import traceback
import getpass
import ccqHubVars


def checkAdminRights():
    import ctypes

    try:
        # Check admin rights on Unix
        isAdmin = os.getuid()
        if isAdmin == 0:
            return {"status": "success", "payload": True}
        else:
            return {"status": "failure", "payload": False}
    except AttributeError:
        # Check admin rights Windows
        isAdmin = ctypes.windll.shell32.IsUserAnAdmin()
        if isAdmin:
            return {"status": "success", "payload": True}
        else:
            return {"status": "failure", "payload": False}


def validateCcqHubAdminKey(ccqHubAdminKeyPath):
    key = ""
    try:
        keyFile = open(os.path.dirname(os.path.realpath(__file__)) + "/.." + str(ccqHubMethods.ccqHubAdminKeyFile), "r")
        key = keyFile.readline()
    except Exception as e:
        # Check if the user has the key that is authorized to perform the actions
        if ccqHubAdminKeyPath is None:
            ccqHubAdminKeyPath = raw_input("Please enter the path to the ccqHubAdmin.key file. This file was generated by the ccqHubInstaller application and by default it is located in the .keys directory where ccqHub is installed. Make sure you have read access to this file before continuing.\n")

        try:
            keyFile = open(str(ccqHubAdminKeyPath), "r")
            key = keyFile.readline()
        except Exception as e:
            #print "got here"
            return {"status": "error", "payload": {"error": "Unable to read the key from the provided file. Please make sure you have the required permissions and try again.", "traceback": traceback.format_exc(e)}}

    return {"status": "success", "payload": key}


def validateCcqHubJobKey(ccqHubJobKeyPath):
    key = ""

    # Try the path provided by the user first.
    if ccqHubJobKeyPath is not None:
        try:
            keyFile = open(str(ccqHubJobKeyPath), "r")
            key = keyFile.readline()
            return {"status": "success", "payload": key}
        except Exception as e:
            pass

    try:
        # Check to see if they have permission to use the ccqHubInstaller generated job key file if so use it, if not check the key they provided and if they did not provide one, prompt them for one before continuing.
        keyFile = open(os.path.dirname(os.path.realpath(__file__)) + "/.." + str(ccqHubMethods.ccqHubAdminJobSubmitKeyFile), "r")
        key = keyFile.readline()
        return {"status": "success", "payload": key}
    except Exception as e:
        # If the user has not provided a key path, ask them for one now.
        # Check if the user has the key that is authorized to perform the actions
        if ccqHubJobKeyPath is None:
            ccqHubJobKeyPath = raw_input("Please enter the path to a ccqHub generated key file with job submission permissions. These files are generated by the ccqHub installation application and should have been given to you by the ccqHub administrator\n")

        try:
            keyFile = open(str(ccqHubJobKeyPath), "r")
            key = keyFile.readline()
            return {"status": "success", "payload": key}
        except Exception as e:
            #print "got here"
            return {"status": "error", "payload": {"error": "Unable to read the key from the provided file. Please make sure you have the required permissions and try again.", "traceback": traceback.format_exc(e)}}


def evaluatePermissions(subject, actions):
    # Get the valid actions and requiredAttributes
    actionsAndRequiredAttributes = policies.getValidActionsAndRequiredAttributes()

    for thing in actions:
        if thing not in actionsAndRequiredAttributes:
            return {"status": "error", "payload": "Invalid action specified"}

    typeOfSubject = subject['subjectType']
    subjectRecType = subject['subjectRecType']
    uuid = ""
    queryInformation = ""
    if str(subjectRecType) == "Identity":
        if typeOfSubject == "key":
            shortKey = str(subject['subject']).split(":")[0]
            queryInformation = "-keyId-" + str(shortKey)

        elif typeOfSubject == "identityUuid":
            queryInformation = ""
            uuid = str(subject['subject'])

        elif typeOfSubject == "userName":
            queryInformation = "-userName-" + str(subject['subject'])

        else:
            return {"status": "error", "payload": "Invalid subject type provided."}

    response = ccqHubMethods.queryObj(None, "RecType-" + str(subjectRecType) + str(queryInformation) + "-name-" + str(uuid), "query", "json", "beginsWith")
    tempRequiredObject = {}
    tempObtainedObject = {}
    if response['status'] == "success":
        results = response['payload']
        for identity in results:
            # We have the identity object
            try:
                # Check to make sure that the key they used is actually on the Identity
                if typeOfSubject == "key":
                    decKeyObj = ccqHubMethods.decryptString(identity['keyInfo'])
                    if decKeyObj['status'] != "success":
                        return {"status": "error", "payload": decKeyObj['payload']}
                    else:
                        if str(subject['subject']) not in decKeyObj['payload']:
                            return {"status": "failure", "payload": "The identity is not authorized to perform the requested action(s)."}
                if typeOfSubject == "userName":
                    #TODO need to check and see if the userName belongs to the identity
                    print "Work In Progress"
            except Exception as e:
                return {"status": "failure", "payload": "Unable to validate the provided key.\n" + str(''.join(traceback.format_exc(e)))}

            for action in actions:
                if action in actionsAndRequiredAttributes:
                    for category in actionsAndRequiredAttributes[action]:
                        #print category
                        if category == "attributes":
                            for attribute in actionsAndRequiredAttributes[action]:
                                if attribute in identity:
                                    tempObtainedObject[attribute] = identity[attribute]
                                if attribute not in tempRequiredObject:
                                    tempRequiredObject[attribute] = {}
                                for other in actionsAndRequiredAttributes[action][attribute]:
                                    tempRequiredObject[attribute][other] = actionsAndRequiredAttributes[action][attribute][other]
                        if category == "groups":
                            #TODO implement a way to check if the identity belongs to the group or not
                            pass
                        else:
                            pass

        # If we find the permission we are looking for pop it off the required object and move on. Have to make temp object
        # because you can't modify a dict you are looping through.
        isValid = 0
        if len(tempObtainedObject) == 0:
            return {"status": "failure", "payload": "This identity is not authorized to perform the requested action(s)."}

        for foundAttribute in tempObtainedObject:
            tempObtainedObject[foundAttribute] = json.loads(tempObtainedObject[foundAttribute])
            for thing in tempObtainedObject[foundAttribute]:
                try:
                    if str(tempObtainedObject[foundAttribute][thing]) == str(tempRequiredObject[foundAttribute][thing]):
                        tempRequiredObject[foundAttribute].pop(thing)
                except Exception as e:
                    pass
            try:
                if len(tempRequiredObject[foundAttribute]) != 0:
                   isValid += 1
            except Exception as e:
                pass

        if isValid > 0:
            return {"status": "failure", "payload": "This identity is not authorized to perform the requested action(s)."}
        else:
            return {"status": "success", "payload": "This identity is authorized to perform the requested action(s)."}
    else:
        return {"status": "error", "payload": response['payload']}


def validateAppKey(ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired):
    identityUuid = None
    response = ccqHubMethods.queryObj(None, "RecType-Identity-keyId-" + str(ccAccessKey.split(":")[0]) + "-name-", "query", "json", "beginsWith")
    if response['status'] == "success":
        results = response['payload']
        for tempItem in results:
            identityUuid = tempItem['name']
            try:
                # Need to load the list of keys from the user and decrypt the object
                results = ccqHubMethods.decryptString(tempItem['keyInfo'])
                if results['status'] != "success":
                    return {"status": "error", "message": results['message']}
                else:
                    decryptedKeys = results['payload']
            except Exception as e:
                decryptedKeys = []

            if len(decryptedKeys) != 0:
                listOfUserKeys = json.loads(decryptedKeys)
            else:
                listOfUserKeys = []

            if str(ccAccessKey) in listOfUserKeys:
                # Need to check and see if the key belongs to a proxyIdentity or not
                isProxy = False
                if str(remoteUserName) != "None":
                    subject = {"subjectType": "key", "subject": str(ccAccessKey), "subjectRecType": "Identity"}
                    results = evaluatePermissions(subject, ["proxyUser"])
                    if results['status'] != "success":
                        return {"status": "error", "payload": results['payload']}
                    else:
                        # The key is authorized to be a proxyUser set the userName to be remoteUserName
                        isProxy = True

                # Evaluate the additionalActions requested if there are any
                for additionalAction in additionalActionsAndPermissionsRequired:
                    subject = {"subjectType": "key", "subject": str(ccAccessKey), "subjectRecType": "Identity"}
                    results = evaluatePermissions(subject, additionalActionsAndPermissionsRequired[additionalAction])
                    print results
                    if results['status'] != "success":
                        return {"status": "failure", "payload": results['payload']}

                return {"status": "success", "payload": {"message": "Successfully validated the key.", "identity": str(identityUuid), "isProxy": isProxy}}
            else:
                #The AccessKey provided is not valid return error
                return {"status": "error", "payload": "App Key not valid."}

        #If the APIKey object isn't found in the DB return not valid
        return {"status": "error", "payload": "App Key not valid."}

    else:
        #If the APIKey object isn't found in the DB return not valid
        return {"status": "error", "payload": "App Key not valid."}


def validateJobAuthParameters(userName, password, appKeyLocation, remoteUserName, additionalActionsAndPermissionsRequired, bypassRemoteUserCheck):
    # TODO need to check to see if the user has access to the auto-generated job submit key and if so use that one. If not need to get the path to the key.
    #Check to see if the user has an API key only if they do not specify a username or password, if they have access open the file and read in the key.
    appKey = None

    # Validate that the correct combination of commandline arguments have been provided.
    if appKeyLocation is not None:
        if userName is not None or password is not None:
            return {"status": "failure", "payload": "The -un/-pw commandline arguments cannot be used with the -i/-ru arguments."}
        else:
            try:
                keyFile = open(str(appKeyLocation), "r")
                appKey = keyFile.readline()

                # Need to try and get the Identity out of the DB. If there is only one userName on it we will use that, if there is more then one it will prompt the user as to which userName to use, or if there is a remoteUsername specified it will check if the user is a proxy user and if so allow them to use the remoteUserName.
                if remoteUserName is None:
                    # Check the Identity object and see how many usernames we have, if there is more than one have the user choose one
                    res = ccqHubMethods.queryObj(None, "RecType-Identity-keyId-" + str(str(appKey).split(":")[0]) + "-name-", "query", "json", "beginsWith")
                    if res['status'] != "success":
                        return {"status": "error", "payload": res['payload']}
                    else:
                        for ident in res['payload']:
                            userNames = json.loads(ident['userName'])
                            if len(userNames) == 1:
                                userName = userNames[0]
                            else:
                                possibleNames = ""
                                for name in userNames:
                                    possibleNames += str(name) + ", "
                                attempts = 0
                                valid = False
                                while attempts < 5:
                                    userName = raw_input("Please specify the username that you want to use with this Identity. The possible values are: " + str(possibleNames) + "\n")
                                    if str(userName) in userNames:
                                        valid = True
                                        attempts = 6
                                    else:
                                        attempts += 1
                                if not valid:
                                    return {"status": "failure", "payload": "Maximum number of input tries reached."}

                if not bypassRemoteUserCheck:
                    if remoteUserName is None and userName is None:
                        values = ccqHubVars.retrieveSpecificConfigFileKey("General", "promptRemoteUserName")
                        if values['status'] != "success":
                            print values['payload']
                            sys.exit(0)
                        else:
                            promptRemoteUserName = values['payload']
                            if str(promptRemoteUserName).lower() == "no":
                                remoteUserName = getpass.getuser()
                            elif str(promptRemoteUserName).lower() == "yes":
                                remoteUserName = ccqHubMethods.getInput("remote username", "username that you want the job to run as on the remote system. This user must exist on the remote system or the job will fail", None, None)
                            else:
                                return {"status": "error", "payload": "Unsupported value found in the config file for the promptRemoteUserName field. This value must be either yes or no."}

                return {"status": "success", "payload": {"userName": userName, "password": password, "appKey": appKey, "remoteUserName": remoteUserName}}
            except Exception as e:
                # The path the user specified is not valid, return failure
                return {"status": "failure", "payload": "Unable to successfully validate the app key in the location provided."}
    else:
        # Check to see if the user has access to the admin job submit key and if so use that. If the user specifies a username/password then we do not check the admin job keys
        if userName is None and password is None:
            try:
                keyFile = open(os.path.dirname(os.path.realpath(__file__)) + "/.." + str(ccqHubMethods.ccqHubAdminJobSubmitKeyFile), "r")
                appKey = keyFile.readline()

                if not bypassRemoteUserCheck:
                    if remoteUserName is None:
                        values = ccqHubVars.retrieveSpecificConfigFileKey("General", "promptRemoteUserName")
                        if values['status'] != "success":
                            print values['payload']
                            sys.exit(0)
                        else:
                            promptRemoteUserName = values['payload']
                            if str(promptRemoteUserName).lower() == "no":
                                remoteUserName = getpass.getuser()
                            elif str(promptRemoteUserName).lower() == "yes":
                                remoteUserName = ccqHubMethods.getInput("remote username", "username that you want the job to run as on the remote system. This user must exist on the remote system or the job will fail", None, None)
                            else:
                                return {"status": "error", "payload": "Unsupported value found in the config file for the promptRemoteUserName field. This value must be either yes or no."}

                return {"status": "success", "payload": {"userName": userName, "password": password, "appKey": appKey, "remoteUserName": remoteUserName}}
            except Exception as e:
                # The user doesn't have access to the admin job key, pass and ask for userName/password
                pass
        # If we didn't find a valid key that we could use, prompt the user for the username/password if they are not passed as command line arguments
        if appKey is None:
            if password is not None:
                if userName is None:
                    userName = raw_input("Please enter your username: \n")

            if userName is not None:
                if password is None:
                    password = getpass.getpass("Please enter your password: \n")

            if userName is None and password is None:
                userName = raw_input("Please enter your username: \n")
                password = getpass.getpass("Please enter your password: \n")

            return {"status": "success", "payload": {"userName": userName, "password": password, "appKey": appKey, "remoteUserName": remoteUserName}}
