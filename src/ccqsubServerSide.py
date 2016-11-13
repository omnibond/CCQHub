# Copyright Omnibond Systems, LLC. All rights reserved.

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

import base64
from random import randint

from bottle import request, route, get, post, error
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
from Slurm import SlurmScheduler
from Torque import TorqueScheduler
from Condor import CondorScheduler
from Openlava import OpenlavaScheduler

import argparse
import urllib2
import json
import traceback
import ccqsubMethods
import commands
import math
from datetime import date, datetime, timedelta
import platform

tempJobScriptLocation = ccqHubMethods.tempScriptJobLocation
tempJobOutputLocation = ccqHubMethods.tempJobOutputLocation

def getInstanceRecType():
    typeOfInstance = ""
    clusterName = ""
    schedType = ""
    instanceId = ""
    schedulerName = ""
    vpcId = ""
    #get the instance ID
    urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id')
    instanceID = urlResponse.read()
    items = ccqHubMethods.queryObject(None, instanceID, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for item in items:
        typeOfInstance = item['RecType']
        clusterName = item['clusterName']
        instanceId = item['instanceID']
        vpcId = item['VPC_id']
        if typeOfInstance == "Scheduler":
            schedType = item['schedType']
            schedulerName = item['instanceName']

    if typeOfInstance != "" and clusterName != "":
        return {'status': 'success', 'payload': {"RecType": str(typeOfInstance), "clusterName": str(clusterName), "instanceId": str(instanceId), "vpcId": str(vpcId), "schedType": schedType, "schedulerName": schedulerName}}

    return {'status': 'error', 'payload': "Error getting the Scheduler Type of this instance and the clusterName from the DB!"}

@route('/ccqstat', method='POST')
def ccqstat():
    VARS = request.json
    jobId = VARS["jobId"]
    userName = VARS["userName"]
    password = VARS["password"]
    verbose = VARS['verbose']
    instanceId = VARS['instanceId']
    jobNameInScheduler = VARS['jobNameInScheduler']
    schedulerName = VARS['schedulerName']
    schedulerType = VARS['schedulerType']
    schedulerInstanceId = VARS['schedulerInstanceId']
    schedulerInstanceName = VARS['schedulerInstanceName']
    schedulerInstanceIp = VARS['schedulerInstanceIp']

    instanceRecType = ""

    #Stuff for if authed from cert file
    isCert = VARS['isCert']
    if str(isCert) == "True":
        values = ccqsubMethods.decodeCertUnPwVals(str(userName), str(password))
        if values['status'] != "success":
            return {"status": "error", "payload": {"error": "There was a problem trying to decode the credentials!", "traceback": ''.join(traceback.format_stack())}}
        else:
            userName = values['payload']['decUname']
            password = values['payload']['decPass']
    else:
        userName = ccqsubMethods.decodeString("ccqunfrval", str(userName))
        password = ccqsubMethods.decodeString("ccqpwdfrval", str(password))

    values = getInstanceRecType()
    if values['status'] != "success":
        return values['payload']
    else:
        instanceRecType = values['payload']['RecType']
        clusterName = values['payload']['clusterName']
        schedType = values['payload']['schedType']
        if instanceRecType == "ControlNode":
            instanceId = values['payload']['instanceId']
        else:
            schedulerName = values['payload']['schedulerName']
        vpcId = values['payload']['vpcId']

    if instanceRecType == "Scheduler":

        #Create scheduler objects that can call that specific scheduler's queue monitoring command and spit back the output
        kwargs = {"schedName": schedulerName, "schedType": schedulerType, "instanceID": schedulerInstanceId, "clusterName": clusterName, "instanceName": schedulerInstanceName, "schedulerIP": schedulerInstanceIp}

        if schedType == "Torque":
            scheduler = TorqueScheduler(**kwargs)
        elif schedType == "Slurm":
            scheduler = SlurmScheduler(**kwargs)
        elif schedType == "Condor":
            scheduler = CondorScheduler(**kwargs)
        elif schedType == "Openlava":
            scheduler = OpenlavaScheduler(**kwargs)

        kwargs = {"jobId": jobId, "jobNameInScheduler": jobNameInScheduler, "userName": userName}
        output = scheduler.getJobStatusFromScheduler(**kwargs)
        return {"status": "success", "payload": output}


    elif instanceRecType == "ControlNode":
        print "Imma control node"
        if jobId != "all":
            values = ccqsubMethods.checkJobIdAndUserValidity(jobId, userName, isCert)
            if values['status'] != "success":
                return {"status": "error", "payload": values['payload']}
            else:
                if not values['payload']['jobExists']:
                    print values['payload']['message']
                    return {"status": str(values['status']), "payload": values['payload']['message']}
                else:
                    jobInformation = values['payload']['jobInformation']
                    if not verbose:
                        print "The current status of job " + str(jobId) + " is " + str(jobInformation['status']) + "!\n To see more information use the -v argument!"
                        return {"status": str(values['status']), "payload": "The current status of job " + str(jobId) + " is " + str(jobInformation['status']) + "!\n To see more information use the -v argument!"}

                    elif verbose and jobInformation['status'] == "Submitted" or jobInformation['status'] != "Running":
                        values = ccqsubMethods.getStatusFromScheduler(jobId, userName, password, verbose, instanceId,
                                                                      isCert, schedulerName)
                        if values['status'] == 'success':
                            return {"status": "success", "payload": values["payload"]}
                        return {"status": "error", "payload": values['payload']}
                    else:
                        print {"status": "success", "payload": "The -v argument is current only available for jobs that are in the Submitted and running states. Verbose information for the Creating, Error, and Queued states will be available in a future release of ccqstat.\nThe current non-verbose status of job " + str(jobId) + " is " + str(jobInformation['status']) + "!\n"}
        else:
            values = ccqsubMethods.getStatusFromScheduler(jobId, userName, password, verbose, instanceId, isCert,
                                                          schedulerName)
            if values['status'] == 'success':
                return {"status": "success", "payload": values["payload"]}
            return {"status": "error", "payload": values['payload']}

@route('/ccqdel', method='POST')
def ccqdel():
    VARS = request.json
    jobId = VARS["jobId"]
    userName = VARS["userName"]
    password = VARS["password"]
    instanceId = VARS["instanceId"]
    jobNameInScheduler = VARS["jobNameInScheduler"]
    jobForceDelete = VARS['jobForceDelete']
    schedulerType = VARS['schedulerType']
    schedulerInstanceId = VARS['schedulerInstanceId']
    schedulerInstanceName = VARS['schedulerInstanceName']
    schedulerInstanceIp = VARS['schedulerInstanceIp']

    #Stuff for if authed from cert file
    isCert = VARS['isCert']
    if str(isCert) == "True":
        values = ccqsubMethods.decodeCertUnPwVals(str(userName), str(password))
        if values['status'] != "success":
            return {"status": "error", "payload": {"error": "There was a problem trying to decode the credentials!", "traceback": ''.join(traceback.format_stack())}}
        else:
            userName = values['payload']['decUname']
            password = values['payload']['decPass']
    else:
        userName = ccqsubMethods.decodeString("ccqunfrval", str(userName))
        password = ccqsubMethods.decodeString("ccqpwdfrval", str(password))

    instanceRecType = ""
    schedulerName = ""

    values = getInstanceRecType()
    if values['status'] != "success":
        return values['payload']
    else:
        instanceRecType = values['payload']['RecType']
        clusterName = values['payload']['clusterName']
        schedType = values['payload']['schedType']
        if instanceRecType == "ControlNode":
            instanceId = values['payload']['instanceId']
        else:
            schedulerName = values['payload']['schedulerName']
        vpcId = values['payload']['vpcId']

    if instanceRecType == "Scheduler":
        #Still needs a way to add a purge flag and to delete from the DB
        print "Imma scheduler"
        #Create scheduler objects that can call that specific scheduler's queue monitoring command and spit back the output

        kwargs = {"schedName": schedulerName, "schedType": schedulerType, "instanceID": schedulerInstanceId, "clusterName": clusterName, "instanceName": schedulerInstanceName, "schedulerIP": schedulerInstanceIp}

        if schedType == "Torque":
            scheduler = TorqueScheduler(**kwargs)
        elif schedType == "Slurm":
            scheduler = SlurmScheduler(**kwargs)
        elif schedType == "Condor":
            scheduler = CondorScheduler(**kwargs)
        elif schedType == "Openlava":
            scheduler = OpenlavaScheduler(**kwargs)

        kwargs = {"jobForceDelete": jobForceDelete, "jobNameInScheduler": jobNameInScheduler}
        output = scheduler.deleteJobFromScheduler(**kwargs)
        print output

        #Really should run some code here to make sure the job is really gone.....

        #Delete the DB object for the job that they want gone.
        results = ccqHubMethods.queryObject(None, jobId, "get", "dict")
        if results['status'] == "success":
            results = results['payload']
        else:
            return {"status": "error", "payload": {"error": "Query Error Exception!\n", "traceback": ''.join(traceback.format_stack())}}
        for DDBItem in results:
            obj = {'action': 'delete', 'obj': DDBItem}
            values = ccqHubMethods.handleObj(obj)
            if values['status'] == "success":
                return {"status": "success", "payload": "The delete command for the job was successfully executed!"}

    elif instanceRecType == "ControlNode":
        print "Imma control node"

        values = ccqsubMethods.checkJobIdAndUserValidity(jobId, userName, isCert)
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}
        else:
            if not values['payload']['jobExists']:
                print values['payload']['message']
                return {"status": str(values['status']), "payload": values['payload']['message']}
            else:
                jobInformation = values['payload']['jobInformation']
                values = ccqsubMethods.deleteJobFromScheduler(jobInformation['name'], userName, password, instanceId, jobForceDelete, isCert)
                return values


@route('/ccqsub', method='POST')
def ccqsub():
    print "Made it to ccqsub!"

    VARS = request.json
    jobScriptLocation = VARS['jobScriptLocation']
    jobScriptText = VARS['jobScriptText']
    ccOptionsParsed = VARS['ccOptionsCommandLine']
    jobId = VARS["jobId"]
    jobName = VARS['jobName']
    jobMD5Hash = VARS["jobMD5"]
    userName = VARS["userName"]
    password = VARS["password"]
    schedName = VARS['schedName']

    print "Past getting vars from ccqsub"

    # Stuff for if authed from cert file
    isCert = VARS['isCert']
    if str(isCert) == "True":
        values = ccqsubMethods.decodeCertUnPwVals(str(userName), str(password))
        if values['status'] != "success":
            print "There was a problem trying to decode the credentials!"
            print userName
            print password
            return {"status": "error", "payload": {"error": "There was a problem trying to decode the credentials!",
                                                   "traceback": ''.join(traceback.format_stack())}}
        else:
            userName = values['payload']['decUname']
            password = values['payload']['decPass']
    else:
        userName = ccqsubMethods.decodeString("ccqunfrval", str(userName))
        password = ccqsubMethods.decodeString("ccqpwdfrval", str(password))

    spotPrice = ccOptionsParsed["spotPrice"]
    useSpot = ccOptionsParsed["useSpot"]
    volumeType = ccOptionsParsed["volumeType"]
    if volumeType == "ssd":
        volumeType = "gp2"
    elif volumeType == "magnetic":
        volumeType = "standard"
    elif volumeType == "ssdiops":
        return {"status": "error",
                "payload": {"error": "The volume type ssdiops is not currently supported by ccqsub at this time!",
                            "traceback": ''.join(traceback.format_stack())}}
    else:
        return {"status": "error", "payload": {
            "error": "The volume type " + str(volumeType) + " is not a valid AWS EBS Volume type please try again!",
            "traceback": ''.join(traceback.format_stack())}}

    schedulerToUse = ccOptionsParsed["schedulerToUse"]
    schedType = ccOptionsParsed["schedType"]

    justPrice = str(ccOptionsParsed["justPrice"])

    # Check to see if the OS is RedHat and if so then make sure the job isn't set to use Spot Instances!
    (name, ver, id) = platform.linux_distribution()
    if useSpot == "yes" and name.replace(" ", "").lower().startswith("redhat"):
        values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
        if values['status'] != "success":
            # Need to update the job status here and somehow notify the user the job has failed
            print {"Error": "QueryErrorException"}
        ClusterMethods.writeToErrorLog(
            {"messages": ["Spot Instances are not supported on RedHat based Instances!\n"],
             "traceback": [''.join(traceback.format_stack())]}, "ccqsubServerSubmit")
        return {"status": "error",
                "payload": "Red Hat instances do not current support the use of Spot Instances. Please re-submit your job not using Spot Instances!"}

    values = getInstanceRecType()
    if values['status'] != "success":
        return values['payload']
    else:
        instanceRecType = values['payload']['RecType']

    if instanceRecType == "ControlNode":
        print "Imma control node"
        if str(justPrice).lower() == "true":
            parameters = {"ccOptionsParsed": ccOptionsParsed}
            output = ccqsubMethods.getInstanceType(parameters)
            if output['status'] != "success":
                print output['payload']['error']
                return {"status": "error", "payload": output['payload']}

            calculatedPrice = 0

            if spotPrice is not None:
                # Need to get spot pricing here!!
                results = ccqsubMethods.calculatePriceForJob(output['payload']['instanceType'],
                                                             ccOptionsParsed["numberOfInstancesRequested"], "37",
                                                             ccOptionsParsed['volumeType'], "OnDemandPrice", True,
                                                             ccOptionsParsed['spotPrice'])
                if results['status'] != "success":
                    print str(results['payload']["error"])
                    return {"status": "error", "payload": results['payload']}
                else:
                    calculatedPrice = round(results['payload'], 3)

            else:
                # Need to get regular pricing here!!
                results = ccqsubMethods.calculatePriceForJob(output['payload']['instanceType'],
                                                             ccOptionsParsed["numberOfInstancesRequested"], "37",
                                                             ccOptionsParsed['volumeType'], "OnDemandPrice", False,
                                                             None)
                if results['status'] != "success":
                    print str(results['payload']["error"])
                    return {"status": "error", "payload": results['payload']}
                else:
                    calculatedPrice = round(results['payload'], 3)

            return {"status": "success",
                    "payload": "The AWS calculated for this job based upon the requested parameters is: " + str(
                        output['payload']['instanceType']) + " with an estimated cost of $" + str(calculatedPrice)}

        schedulerIpInfo = ccqsubMethods.getSchedulerIPInformation(schedulerToUse, schedType)
        if schedulerIpInfo['status'] == 'success':
            schedulerIpAddress = schedulerIpInfo['payload']["schedulerIpAddress"]
            schedulerType = schedulerIpInfo['payload']['schedulerType']
            schedulerName = schedulerIpInfo['payload']['schedName']
            schedulerInstanceId = schedulerIpInfo['payload']['schedulerInstanceId']
            schedulerInstanceName = values['payload']['instanceName']

            ccOptionsParsed["schedulerToUse"] = str(schedulerName)
            ccOptionsParsed['schedType'] = str(schedulerType)

        else:
            print "Unable to obtain the information about the " + str(
                schedulerToUse) + " Scheduler that was requested! Please try again in a few minutes!"
            sys.exit(0)

        # Already decoded up at the top so no reason to re-decode it.
        obj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptFile": str(jobScriptText),
               "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash,
               "userName": str(userName), "password": str(password), "isCert": str(isCert),
               "isRemoteSubmit": "True"}
        values = ccqsubMethods.readyJobForScheduler(obj)
        return {"status": str(values['status']), "payload": values["payload"]}


@route('/gencclogincert', method='POST')
def gencclogincert():
    print "Obtaining new Certificate!"
    VARS = request.json
    userName = VARS["userName"]
    password = VARS["password"]
    certLength = VARS["certLength"]

    userName = ccqsubMethods.decodeString("ccqunfrval", str(userName))
    password = ccqsubMethods.decodeString("ccqpwdfrval", str(password))

    certEncodedUser = ""
    certEncodedPass = ""

    #Create the new certificate to pass back to the user
    newExpireDate = datetime.now() + timedelta(days=int(certLength))

    encryptedExpireDate = ccqsubMethods.encodeString("ccqdatstrfrcrtfil", str(newExpireDate))

    stringObjU = {"string": str(userName)}
    certEncodedUser = encString(stringObjU)
    if certEncodedUser['status'] != "success":
        return {"status": "error", "payload": "There was an error trying to create the new certificate!"}
    else:
        certEncodedUser = certEncodedUser['payload']

    stringObjP = {"string": str(password)}
    certEncodedPass = encString(stringObjP)
    if certEncodedPass['status'] != "success":
        return {"status": "error", "payload": "There was an error trying to create the new certificate!"}
    else:
        certEncodedPass = certEncodedPass['payload']
    #Create Validation Key
    p1 = str(newExpireDate).split(" ")[1]

    count = 0
    uPlace = 0
    pPlace = 0

    finV = ""
    for l in p1:
        finV += str(l)
        if count % 2 == 0 and uPlace < len(userName):
            finV += str(userName[uPlace])
            uPlace += 1
        elif pPlace < len(password):
            finV += str(password[pPlace])
            pPlace += 1
        count += 1

    fullCertPass = ""
    itemCount = 0
    for key, value in certEncodedPass.iteritems():
        if itemCount == 0:
            fullCertPass += str(value)
        else:
            fullCertPass += ":" + str(value)
        itemCount += 1

    fullCertUser = ""
    itemCount = 0
    for key, value in certEncodedUser.iteritems():
        if itemCount == 0:
            fullCertUser += str(value)
        else:
            fullCertUser += ":" + str(value)
        itemCount += 1

    fullCertPass = base64.urlsafe_b64encode(fullCertPass)
    fullCertUser = base64.urlsafe_b64encode(fullCertUser)
    finV = base64.urlsafe_b64encode(finV)

    return {"status": "success", "payload": {"userName": str(fullCertUser), "password": str(fullCertPass), "valKey": str(finV), "dateExpires": str(encryptedExpireDate)}}

#Route to allow validation of users who aren't coming from a CC instance. This way we don't have to modify all the existing
#functions for if the user is on a CC instance.
@route('/validateccqcreds', method='POST')
def gencclogincert():
    print "Checking user Credentials!"
    VARS = request.json
    userName = VARS["userName"]
    password = VARS["password"]
    dateExpires = VARS["dateExpires"]

    valKey = VARS['valKey']

    validUser = False

    #Need to do checks here to make sure the user is authed
    if valKey != "unpw":
        print "Checking Certificate......."
        values = ccqsubMethods.validateCertificate(userName, password, valKey, dateExpires)
        if values['status'] == "success":
            validUser = True
        else:
            print values['payload']
            return {"status": "error", "payload": "Invalid certificate credentials provided! Please delete the old certificate and try again using a username and password!"}

    else:
        decodedUserName = ccqsubMethods.decodeString("ccqunfrval", str(userName))
        decodedPassword = ccqsubMethods.decodeString("ccqpwdfrval", str(password))

        values = ccqsubMethods.checkUserNamePassword(decodedUserName, decodedPassword)
        if values['status'] != 'success':
            print values['payload']
        else:
            print values['payload']
            validUser = True

    if not validUser:
        return {"status": "error", "payload": "Invalid username or password!"}
    else:
        return {"status": "success", "payload": "The login attempt was successful!"}
