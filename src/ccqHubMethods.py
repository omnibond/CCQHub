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
import base64
import commands
import os
import socket
import sys
import time
import traceback
import urllib2
import json
from random import randint
import datetime
from datetime import timedelta
import ccqHubVars
import policies
import threading
import ConfigParser

# sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
# from Slurm import SlurmScheduler
# from Torque import TorqueScheduler
# from Condor import CondorScheduler
# from Openlava import OpenlavaScheduler

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Database"))
from sqlLite3Database import sqlLite3Database

ccqHubVars.init()
if ccqHubVars.ccqHubDBLock is None:
    ccqHubVars.ccqHubDBLock = threading.RLock()

if ccqHubVars.databaseType is None or ccqHubVars.databaseType == "sqlite3":
    dbInterface = sqlLite3Database()
else:
    print "Database type not supported at this time."
    sys.exit(0)

ccqHubKeyDir = "/.keys"
ccqHubKeyFile = str(ccqHubKeyDir) + "/ccqHubEnc.key"
ccqHubAdminKeyFile = str(ccqHubKeyDir) + "/ccqHubAdmin.key"
ccqHubAdminJobSubmitKeyFile = str(ccqHubKeyDir) + "/ccqHubJobSubmit.key"

encryptAlgNum = "a"

# List of supported scheduler types. This is required in order to create a default target for each scheduler type.
ccqHubSupportedSchedulerTypes = ["torque", "slurm", "ccq"]

ccqHubSupportedProtocolTypes = ["http"]#,"local", "ssh"]

ccqHubSupportedAuthenticationTypes = ["appkey"]#, "ssh", "username"]


########################################################################################################################
#                         Database Methods that call the chosen DB interface                                           #
########################################################################################################################
def queryObj(limit, key, action, returnType, filter=None):
    kwargs = {'key': key, 'action': action, 'returnType': returnType, "limit": limit, 'filter': filter}
    return dbInterface.queryObj(**kwargs)


def handleObj(*args):
    return dbInterface.handleObj(*args)


def addObj(*args):
    return dbInterface.addObj(*args)


def deleteObj(*args):
    return dbInterface.addObj(*args)


def addIndexes(*args):
    return dbInterface.addIndexes(*args)


def deleteIndexes(*args):
    return dbInterface.deleteIndexes(*args)


def createTable(*args):
    return dbInterface.createTable(*args)


def tableConnect(*args):
    return dbInterface.tableConnect(*args)


def generateTableNames():
    return dbInterface.generateTableNames()

########################################################################################################################
#                                      Methods that deal with job management                                           #
########################################################################################################################


def updateJobInDB(fieldsToAddToJob, jobId):
    #Update the job DB entry with the status of the job!
    done = False
    timeToWait = 10
    maxTimeToWait = 120
    timeElapsed = 0
    quit = False
    while not done:
        try:
            results = dbInterface.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
            if results['status'] == "success":
                results = results['payload']
            else:
                #Need to update the job status here and somehow notify the user the job has failed
                print "Error: QueryErrorException! Unable to get Item!"
                return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
            for job in results:
                if fieldsToAddToJob is not None and job['status'] != "Deleting":
                    for key, value in fieldsToAddToJob.iteritems():
                        if key == "status" and value == "Deleting":
                            job[key] = value
                            quit = True
                        if not quit:
                            job[key] = value
                job.save()
            if not quit:
                return {"status": "success", "payload": "Success!"}
            elif quit:
                return {"status": "deleting", "payload": "This job has been deleted by the user and should stop execution and all scaling activities!"}
        except Exception as e:
            if timeElapsed >= maxTimeToWait:
                return {"status": "error", "payload": "Failed to save out job status!"}
            time.sleep(timeToWait)
            timeElapsed += timeToWait


def calculateAvgRunTimeAndUpdateDB(startTime, endTime, instanceType, jobName):
    #Update the job DB entry with the status of the job!
    done = False
    timeToWait = 10
    maxTimeToWait = 120
    timeElapsed = 0
    while not done:
        try:
            results = dbInterface.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
            if results['status'] == "success":
                results = results['payload']
            else:
                #Need to update the job status here and somehow notify the user the job has failed
                print "Error: QueryErrorException! Unable to get Item!"
                return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
            for jobScript in results:
                runTimes = jobScript['runTimes']
                try:
                    previousAvg = runTimes[instanceType]['avgTime']
                    previousRunTimes = runTimes[instanceType]['runTimes']
                except KeyError:
                    previousAvg = 0
                    previousRunTimes = []

                if previousAvg == 0:
                    td = int(endTime) - int(startTime)
                    td = timedelta(seconds=td)
                    hours, remainder = divmod(td.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    jobScript['runTimes'][instanceType]['avgTime'] = {"days": int(td.days), "hours": int(hours), "minutes": int(minutes), "seconds": int(seconds)}
                    previousRunTimes.append({"startTime": startTime, "endTime": endTime})
                else:
                    #Need to figure out how to do the average run time of the two date time objects
                    thisRun = int(endTime) - int(startTime)
                    prevAvg = timedelta(days=previousAvg['days'], hours=previousAvg['hours'], minutes=previousAvg['minutes'], seconds=previousAvg['seconds'])
                    newAvgSeconds = (prevAvg.seconds + thisRun.seconds) / 2
                    newAvg = timedelta(seconds=newAvgSeconds)
                    hours, remainder = divmod(newAvg.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    jobScript['runTimes'][instanceType]['avgTime'] = {"days": int(newAvg.days), "hours": int(hours), "minutes": int(minutes), "seconds": int(seconds)}

                jobScript.save()
            return {"status": "success", "payload": "Successfully calculated the new average times!"}
        except Exception as e:
            if timeElapsed >= maxTimeToWait:
                return {"status": "error", "payload": "Failed to save out job script averages status!"}
            time.sleep(timeToWait)
            timeElapsed += timeToWait


def appendSuffixToJobScriptName(jobName):
    #There are conflicting job names who's MD5 hashes do not match so here we append a number to the end of the
    #jobName in order to make it unique
    items = dbInterface.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    count = 0
    for jobScript in items:
        count += 1

    try:
        temp = str(jobName).split('.')
        jobName = str(temp[0])+str(count)
        for x in range(len(temp)):
            if x == 0:
                pass
            else:
                jobName += str(temp[x])
    except:
        jobName = str(jobName)+str(count)

    return {"status": "success", "payload": {"jobName": jobName}}


def compareJobScriptsMD5s(jobName, newJobMD5):
    conflictingJobMD5 = ""
    items = dbInterface.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print items['payload']
        return {'status': 'error', 'payload': str(items['payload']['error']) + ". Traceback: " + str(items['payload']['traceback'])}

    for jobScript in items:
        conflictingJobMD5 = jobScript['jobMD5Hash']
    if conflictingJobMD5 == "":
        return {"status": "error", "payload": {"There was an error retrieving the MD5 Hash for the Job!"}}
    if newJobMD5 == conflictingJobMD5:
        return {"status": "success", "payload": {"sameJob": True}}
    else:
        return {"status": "success", "payload": {"sameJob": False}}


def checkUniqueness(typeToCompare, parameter, jobName=None):
    if typeToCompare == "jobScript":
        items = queryObj(None, "RecType-JobScript-name-" + str(parameter), "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item."
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item."}

        for jobScript in items:
            if jobScript['name'] == parameter:
                return {"status": "success", "payload": {"isTaken": True}}
        return {"status": "success", "payload": {"isTaken": False}}

    elif typeToCompare == "jobId":
        items = dbInterface.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': items['payload']}

        for jobId in items:
            if jobId['name'] == parameter or jobId['jobName'] == jobName:
                return {"status": "success", "payload": {"isTaken": True}}
        return {"status": "success", "payload": {"isTaken": False}}


def saveJobScript(jobScriptLocation, jobScriptText, ccOptionsCommandLine, jobName, jobMD5Hash, userName, targetName, identity, keyPrefix):
    jobInDBAlready = False
    values = checkUniqueness("jobScript", jobName)
    if values['status'] == 'success' and values['payload']['isTaken']:
        results = compareJobScriptsMD5s(jobName, jobMD5Hash)
        if results['status'] != 'success':
            return {"status": "error", "payload": results['payload']}
        else:
            if not results['payload']['sameJob']:
                output = appendSuffixToJobScriptName(jobName)
                if output['status'] != 'success':
                    return {"status": "error", "payload": output['payload']}
                else:
                    jobName = output['payload']['jobName']
            else:
                jobInDBAlready = results['payload']['sameJob']

    if not jobInDBAlready:
        #Save the job script object to the DB
        obj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsParsed": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(userName), "targetName": str(targetName), "identity": str(identity), "keyPrefix": str(keyPrefix)}
        values = putJobScriptInDB(**obj)
        if values['status'] == 'success':
            print values['payload']
        else:
            return {"status": "error", "payload": values['payload']}


def saveJob(jobScriptLocation, jobScriptText, ccOptionsParsed, jobName, userName, isRemoteSubmit, targetName, identity):
    instanceType = ccOptionsParsed["instanceType"]
    jobWorkDir = ccOptionsParsed["jobWorkDir"]

    generatedJobId = ""
    done = False
    while not done:
        jobNums = [randint(0, 9) for p in range(0, 4)]
        for digit in jobNums:
          generatedJobId += str(digit)

        values = checkUniqueness("jobId", generatedJobId)
        if values['status'] == "success" and not values['payload']['isTaken']:
            done = True

    suffixToAddToJobName = 0
    done = False
    while not done:
        if suffixToAddToJobName == 0:
            values = checkUniqueness("jobId", generatedJobId, jobName)
        else:
            jobName = str(jobName)+str(suffixToAddToJobName)
            values = checkUniqueness("jobId", generatedJobId, str(jobName))
        if values['status'] == "success" and not values['payload']['isTaken']:
            done = True
        suffixToAddToJobName += 1

    if str(ccOptionsParsed["stdoutFileLocation"]) == "default":
        stdoutFileLocation = str(jobWorkDir) + str(jobName) + str(generatedJobId) + ".o"
    else:
        stdoutFileLocation = ccOptionsParsed["stdoutFileLocation"]
    if str(ccOptionsParsed["stderrFileLocation"]) == "default":
        stderrFileLocation = str(jobWorkDir) + str(jobName) + str(generatedJobId) + ".e"
    else:
        stderrFileLocation = ccOptionsParsed["stderrFileLocation"]

    obj = {'action': "create"}
    data = {'name': str(generatedJobId), "jobName": str(jobName), 'RecType': 'Job', "targetName": str(targetName), 'schedType': str(ccOptionsParsed['schedType']), 'jobScriptText': str(jobScriptText), 'jobScriptLocation': str(jobScriptLocation), "dateSubmitted": str(time.time()), "startTime": time.time(), "instanceType": str(instanceType), "userName": str(userName), "stderrFileLocation": str(stderrFileLocation), "stdoutFileLocation": str(stdoutFileLocation), "isRemoteSubmit": str(isRemoteSubmit), "jobWorkDir": str(jobWorkDir), "status": "Pending", "identity": str(identity)}
    for command in ccOptionsParsed:
        if ccOptionsParsed[command] != "None":
            data[command] = ccOptionsParsed[command]
    obj['obj'] = data
    response = handleObj(obj)
    if response['status'] == "success" or response['status'] == "partial":
        item = response['payload']
        done = False
        timeToWait = 10
        maxTimeToWait = 120
        timeElapsed = 0
        while not done:
            try:
                items = queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
                if items['status'] == "success":
                    items = items['payload']
                else:
                    print "Error: QueryErrorException! Unable to get Item."
                    return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item."}

                for jobScript in items:
                    jobScript['numberOfTimesRun'] = int(jobScript['numberOfTimesRun'])+1
                    jobScript.save()
                done = True
            except Exception as e:
                print traceback.format_exc(e)
                if timeElapsed >= maxTimeToWait:
                    return {"status": "error", "payload": "Failed to update number of times the job has run."}
                time.sleep(timeToWait)
                timeElapsed += timeToWait

        return {"status": "success", "payload": {"jobId": str(generatedJobId)}}
    else:
        print response['payload']['error']
        print response['payload']['traceback']
        return {"status": "error", "payload": {str(response['payload']['error'])}}


def getStatusFromScheduler(jobId, userName, password, verbose, instanceId, isCert, schedName=None):
    #This will hit the Scheduler Web Service with the job parameters and object and then timeout and return the
    #generated job Id
    if jobId == "all" and schedName is not None:
        schedulerIpAddress = ""
        schedType = ""
        schedulerInstanceName = ""
        schedulerInstanceId = ""

        items = dbInterface.queryObj(None, "RecType-Scheduler-schedName-" + str(schedName), "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            schedulerIpAddress = scheduler['instanceIP']
            schedType = scheduler['schedType']
            schedulerInstanceId = scheduler['instanceID']
            schedulerInstanceName = scheduler['instanceName']

        if str(isCert) == "False":
                password = encodeString("ccqpwdfrval", str(password))
                userName = encodeString("ccqunfrval", str(userName))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqstat"


        final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": None, "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": res}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There was an error trying to get the status of the jobs running on the Scheduler " + str(schedName) + "! " + str(ex)}

    # We will have to validate the user/password/key at some point however the way it is currently done isn't going to work
    # Probably going to have to go hit the server and try and auth things that way kinda like how ccq in the cloud works
    # values = checkJobIdAndUserValidity(jobId, userName, isCert)
    # if values['status'] != "success":
    #     return {"status": "error", "payload": values['payload']}
    # else:
    #     if not values['payload']['jobExists']:
    #         print values['payload']['message']
    #         return {"status": "error", "payload": values['payload']['message']}
    #     else:
    #         jobInformation = values['payload']['jobInformation']

    #TODO we are going to have to go out to the other scheduler/db to get the information about the job
    jobInformation ={}

    schedulerToUse = jobInformation['schedulerUsed']
    schedType = jobInformation['schedType']

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}

    instanceClusterName = None

    items = dbInterface.queryObj(None, instanceId, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        instanceClusterName = None
    for item in items:
        instanceClusterName = item['clusterName']

    if instanceClusterName is not None and instanceClusterName == clusterName:
        if str(isCert) == "False":
            userName = encodeString("ccqunfrval", str(userName))
            password = encodeString("ccqpwdfrval", str(password))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqstat"
        try:
            jobInformation['schedulerJobName']
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": jobInformation['schedulerJobName'], "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        except:
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": None, "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": res}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There ways an error trying to get the status job! " + str(ex)}
    else:
        return {"status": "partial-failure", "payload": "There was an error encountered trying to get the verbose information from the scheduler! The job you requested information on is running on a Cluster that is not the one you are currently using!\n"
                                                        "The non-verbose status of the job " + str(jobId) + " is " + str(jobInformation['status'])}


def deleteJobFromScheduler(jobId, userName, password, instanceId, jobForceDelete, isCert):
    #This will hit the Scheduler Web Service with the job parameters and object and then timeout and return the
    #generated job Id

    #TODO This is going to have to go out and validate with the server and everything before deleting a job so this could be interesting
    # values = checkJobIdAndUserValidity(jobId, userName, isCert)
    # if values['status'] != "success":
    #     return {"status": "error", "payload": values['payload']}
    # else:
    #     if not values['payload']['jobExists']:
    #         print values['payload']['message']
    #         return {"status": "error", "payload": values['payload']['message']}
    #     else:
    #         jobInformation = values['payload']['jobInformation']

    jobInformation = {}

    schedulerToUse = jobInformation['schedulerUsed']
    schedType = jobInformation['schedType']

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}

    instanceClusterName = None

    items = dbInterface.queryObj(None, instanceId, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        instanceClusterName = None
    for item in items:
        instanceClusterName = item['clusterName']

    if instanceClusterName is not None and instanceClusterName == clusterName:
        if str(isCert) == "False":
                password = encodeString("ccqpwdfrval", str(password))
                userName = encodeString("ccqunfrval", str(userName))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqdel"
        try:
            jobInformation['schedulerJobName']
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "instanceId": instanceId, "jobNameInScheduler": jobInformation['schedulerJobName'], "jobForceDelete": jobForceDelete, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        except:
            values = updateJobInDB({"status": "Deleting"}, jobId)
            return {"status": "success", "payload": "The job that you wanted to delete is currently in the process of being deleted!"}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": str(res)}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There was an error trying to delete job! " + str(ex)}
    else:
        return {"status": "partial-failure", "payload": "There was an error encountered trying to delete the job! The job you want to delete on is running on a Cluster that is not the one you are currently using!\n"}


def getSchedulerAndSchedTypeFromJob(jobId):
    results = dbInterface.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        #Need to update the job status here and somehow notify the user the job has failed
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
    for job in results:
        try:
            schedulerToUse = job['schedulerToUse']
            schedType = job['schedType']

            schedulerIpInfo = getSchedulerIPInformation(schedulerToUse, schedType)
            if schedulerIpInfo['status'] == 'success':
                schedulerIpAddress = schedulerIpInfo['payload']["schedulerIpAddress"]
                schedulerInstanceId = schedulerIpInfo['payload']['schedulerInstanceId']
                schedulerInstanceName = schedulerIpInfo['payload']['instanceName']
                return {"status": "success", "payload": str(schedulerIpAddress)}
            else:
                return {"status": "error", "payload": str(schedulerIpInfo['payload'])}
        except:
            return {'status': 'error', 'payload': "Unable to get scheduler information via the Job entry in the DB!"}

    return {"status": "error", "payload": "The job Id requested does not exist in the Database!"}


def getSchedulerIpByName(schedName):
    if schedName == "default":
        items = dbInterface.queryObj(None, "RecType-Scheduler-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            if scheduler['defaultScheduler'] == "true":
                return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP'])}}

        return {'status': 'error', 'payload': "The requested default scheduler was not found in the Database!"}

    items = dbInterface.queryObj(None, "RecType-Scheduler-schedName-" + str(schedName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for scheduler in items:
        return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP'])}}

    #Need to eventually figure out what the default Scheduler for the different types are and if a Scheduler Name isn't
    #specified then we will choose the default Scheduler for that certain type of job

    return {'status': 'error', 'payload': "The requested scheduler was not found in the Database!"}


def calculatePriceForJob(instanceType, numberOfInstances, instanceVolumeSize, instanceVolumeType, purchaseType, isSpotJob, spotPrice):
    try:
        import Cluster
        numberOfInstances = float(numberOfInstances)
        instanceVolumeSize = float(instanceVolumeSize)

        volumeTypes = {"ssd": "gp2", "magnetic": "Magnetic"}

        totalPrice = 0
        storagePricingObj = Cluster.getStoragePricing()
        if storagePricingObj['status'] != 'success':
            return {'status': "error", "payload": storagePricingObj['payload']}
        else:
            storagePricingObj = storagePricingObj['payload']

        cloudyPricing = Cluster.getCloudyPricing()
        if cloudyPricing['status'] != 'success':
            return {'status': "error", "payload": cloudyPricing['payload']}
        else:
            cloudyPricing = cloudyPricing['payload']

        try:
            storagePrice = float(storagePricingObj[str(volumeTypes[instanceVolumeType]).lower()])
            cloudyPrice = float(cloudyPricing[instanceType])

            storagePrice = (storagePrice * instanceVolumeSize) * numberOfInstances
            cloudyPriceTotal = (numberOfInstances * cloudyPrice)

            if not isSpotJob:
                instancePricingObj = Cluster.getInstancePricing()
                if instancePricingObj['status'] != 'success':
                    return {'status': "error", "payload": instancePricingObj['payload']}
                else:
                    instancePricingObj = instancePricingObj['payload']

                instancePrice = float(instancePricingObj[instanceType][purchaseType])

                instancePriceTotal = float(numberOfInstances * instancePrice)

                totalPrice = float(cloudyPriceTotal + instancePriceTotal + storagePrice)

            elif isSpotJob and spotPrice is not None:
                spotPrice = float(spotPrice)
                instancePriceTotal = float(numberOfInstances * spotPrice)

                totalPrice = float(cloudyPriceTotal + instancePriceTotal + storagePrice)

            return {"status": "success", "payload": totalPrice}

        except Exception as e:
            print "There was an error trying to determine the cost of the job!"
            print traceback.print_exc(e)
            return {"status": "error", "payload": {"error": "There was a problem trying to calculate the cost of the job!", "traceback": str(traceback.format_exc(e))}}

    except ImportError as e:
        print "Unable to determine price! Not running on a CC instance!"
        return {"status": "error", "payload": {"error": "Unable to calculate pricing for the job! CCQ not running on a CC instance!", "traceback": str(traceback.format_exc(e))}}


def getInput(fieldName, description, possibleValues, exampleValues):
    inputPrompt = "\nPlease enter a " + str(fieldName) + ". The " + str(fieldName) + " is " + str(description) + ".\n"
    if possibleValues is not None:
        inputPrompt += "The possible values are: "
        tempString = ""
        for x in range(len(possibleValues)):
            if len(possibleValues) == 1:
                tempString = str(possibleValues[x]) + ".\n"
            elif x < len(possibleValues) - 1:
                tempString += str(possibleValues[x]) + ", "
            else:
                tempString += "and " + str(possibleValues[x]) + ".\n"
        inputPrompt += tempString

        done = False
        attempts = 0
        temp = ""
        while attempts < 5 and not done:
            temp = raw_input(inputPrompt)
            if str(temp).lower() in possibleValues:
                done = True
            else:
                print "Invalid selection.\n"
                inputPrompt = "Please select a value from the list: " + tempString
        if not done:
            print "Maximum number of input tries reached, please try again."
            sys.exit(0)
        else:
            return temp
    elif exampleValues is not None:
        inputPrompt += "Some example values include: "
        for x in range(len(exampleValues)):
            if len(exampleValues) == 1:
                inputPrompt += str(exampleValues[x]) + ".\n"
            if x < len(exampleValues) - 1:
                inputPrompt += str(exampleValues[x]) + ", "
            else:
                inputPrompt += "and " + str(exampleValues[x]) + ".\n"
        temp = raw_input(inputPrompt)
        return temp


#TODO need to finish fleshing this out, this may not be exactly what we are going for here.........I'm not sure we may want each key to be it's own identity object
def createIdentity(identityName, actions):
    import uuid
    identityUuid = str(uuid.uuid4())
    obj = {'action': "create", 'obj': {"RecType": "Identity", "name": str(identityUuid), "userName": [], "keyInfo": [], "keyId": [], "identityName": str(identityName)}}
    validActions = policies.getValidActionsAndRequiredAttributes()
    for action in actions:
        if action in validActions:
            for attribute in validActions[action]:
                obj['obj'][attribute] = validActions[action][attribute]
        else:
            return {"status": "failure", "payload": "The action requested: " + str(action) + " is not a valid ccqHub action. Unable to create the new ccqHub identity."}

    # Save out the key to the DB, it has it's own object for storing key permissions and other information
    res = dbInterface.handleObj(**obj)
    if res['status'] != "success":
        return {"status": "error", "payload": res['payload']}
    else:
        return {"status": "success", "message": "Successfully created the new ccqHubIdentity: " + str(identityName) + str("."), "payload": None}


def createDefaultTargetsObject():
    obj = {'action': "create", 'obj': {"RecType": "DefaultTargets", "name": "DefaultTargets"}}
    for schedType in ccqHubSupportedSchedulerTypes:
        obj['obj'][schedType] = "None"

    # Save out the DefaultTarget object to the DB.
    res = dbInterface.handleObj(**obj)
    if res['status'] != "success":
        return {"status": "error", "payload": res['payload']}
    else:
        return {"status": "success", "message": "Successfully created the deafultTarget object.", "payload": None}


def formatCcqstatOutput(jobs):
    headerText = "Id            Name                        Scheduler           Status\n"
    headerText += "--------------------------------------------------------------------\n"
    returnString = ""
    jobTimes = {}
    statusOfEachJob = {}
    for job in jobs:
        jobTimes[job['name']] = job['dateSubmitted']
        jobId = job['name']
        jobName = job['jobName']
        jobStatus = job['status']
        jobScheduler = job['schedulerUsed']
        if len(jobId) > 5:
            #Job Id is too long and has to be truncated for formatting purposes and padding added to the end
            jobId = str(jobId[0:5]) + "..." + (" " * 6)
        else:
            #Make the job id 8 chars long and then add the padding to it
            jobId = jobId + (" " * (8-len(jobId))) + (" " * 6)
        if len(jobName) > 17:
            #Job name is too long and has to be truncated for formatting purposes and padding added to the end
            jobName = str(jobName[0:17]) + "..." + (" " * 8)
        else:
            #Make the job name 20 chars long and then add the padding to it
            jobName = jobName + (" " * (20-len(jobName))) + (" " * 8)
        if len(jobScheduler) > 12:
            # Job name is too long and has to be truncated for formatting purposes and padding added to the end
            jobScheduler = str(jobScheduler[0:12]) + "..." + (" " * 5)
        else:
            # Make the job name 12 chars long and then add the padding to it
            jobScheduler = jobScheduler + (" " * (15 - len(jobScheduler))) + (" " * 5)

        statusOfEachJob[job['name']] = str(jobId) + str(jobName) + str(jobScheduler) + str(jobStatus) + "\n"

    if len(statusOfEachJob) > 0:
        sortedJobsBySubmissionTime = sorted(jobTimes.items(), key=lambda x: x[1])
        for sortedJob in sortedJobsBySubmissionTime:
            returnString += statusOfEachJob[sortedJob[0]]
        return {"status": "success", "payload": headerText + returnString}
    else:
        return {"status": "success", "payload": "There are currently no jobs in the queue."}


def readSubmitHostOutOfConfigFile():
    parser = ConfigParser.ConfigParser()

    if os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + "/../etc/ccqHub.conf"):
        ccqHubConfigFileLocation = os.path.dirname(os.path.realpath(__file__)) + "/../etc/ccqHub.conf"
        try:
            parser.read(ccqHubConfigFileLocation)
            submitHost = parser.get("Web Server", "host")
            port = parser.get("Web Server", "port")
            return {"status": "success", "payload": {"port": port, "host": str(submitHost)}}
        except Exception as e:
            return {"status": "error", "payload": "An error occurred trying to read the configuration file.\n" + str(traceback.format_exc(e))}
    else:
        return {"status": "error", "payload": "Unable to read the host from the configuration file."}

########################################################################################################################
#                                     Methods that deal with encryption and app Keys                                   #
########################################################################################################################


#TODO this may need to be broken out into addUserNameToIdentity and generateAndAddKeyToIdentity.....not sure on that either
def saveAndGenNewIdentityKey(identityName, keyId, actions):
    import hashlib
    import binascii
    import uuid
    try:
        # Generate the new keyUuid
        validKey = False
        attempts = 0
        keyUuid = ""
        while not validKey:
            keyUuid = str(uuid.uuid4().get_hex()[0:6])
            #Check and see if the generated keyUuid is already taken or not
            res = queryObj(None, "RecType-Identity-keyId-" + str(keyUuid) + "-name-", "query", "json", "beginsWith")
            if res['status'] == "success":
                keyList = res['payload']
                if len(keyList) == 0:
                    validKey = True
                else:
                    attempts += 1
            else:
                attempts += 1
            if attempts >= 5:
                return {"status": "error", "message": "Unable to generate the identity app key please try again later.", "payload": None}

        dk = hashlib.pbkdf2_hmac('sha256', str(uuid.uuid4()), os.urandom(128), 100000)
        #Add the newly generated key and the key's permissions to the key object
        #TODO in the future we may add the ability to create ccqHub Users and give them permissions within ccqHub
        identityUuid = str(uuid.uuid4())
        key = binascii.hexlify(dk)

        fullKey = str(keyUuid) + ":" + str(encryptAlgNum) + ":" + str(key)

        if keyId is not None or identityName is not None:
            # The user wants to add this key to the same object as the user or key name passed, we need to modify the object
            # The user cannot add any new actions to this Identity just a new key that has the same permissions
            queryString = ""
            if keyId is not None:
                queryString = "keyId-" + str(keyId)
            elif identityName is not None:
                queryString = "identityName-" + str(identityName)
            obj = {}
            res = queryObj(None, "RecType-Identity-" + str(queryString) + "-name-", "query", "json", "beginsWith")
            if res['status'] == "success":
                objectList = res['payload']
                for object in objectList:
                    if identityName is not None:
                        userNames = json.loads(object['userName'])
                        userNames.append(str(identityName))
                        object['userName'] = json.dumps(userNames)
                    elif keyId is not None:
                        decryptedKeyList = decryptString(object['keyInfo'])
                        if decryptedKeyList['status'] != "success":
                            return {"status": "error", "message": "Unable to generate the identity app key please try again later.", "payload": decryptedKeyList['payload']}
                        else:
                            keyList = json.loads(decryptedKeyList['payload'])
                            keyList.append(fullKey)

                            keyIdList = json.loads(object['keyId'])
                            keyIdList.append(keyUuid)
                            object['keyId'] = keyIdList

                            encryptedKeyObj = encryptString(json.dumps(keyList))
                            if encryptedKeyObj['status'] != "success":
                                return {"status": "error", "message": "Unable to generate the identity app key please try again later.", "payload": encryptedKeyObj['payload']}
                            else:
                                object['keyInfo'] = encryptedKeyObj['payload']

                    obj = {'action': "modify", 'obj': object}
        else:
            encryptedKeyObj = encryptString(json.dumps([str(fullKey)]))
            if encryptedKeyObj['status'] != "success":
                return {"status": "error", "message": "Unable to generate the identity app key please try again later.", "payload": encryptedKeyObj['payload']}
            else:
                obj = {'action': "create", 'obj': {"RecType": "Identity", "name": str(identityUuid), "userName": [], "keyInfo": encryptedKeyObj['payload'], "keyId": [str(keyUuid)]}}
                validActions = policies.getValidActionsAndRequiredAttributes()
                for action in actions:
                    if action in validActions:
                        for attribute in validActions[action]:
                            obj['obj'][attribute] = validActions[action][attribute]
                    else:
                        return {"status": "failure", "payload": "The action requested: " + str(action) + " is not a valid ccqHub action. Unable to generate new user key."}

        # Save out the key to the DB, it has it's own object for storing key permissions and other information
        res = dbInterface.handleObj(**obj)
        if res['status'] != "success":
            return {"status": "error", "payload": res['payload']}
        else:
            return {"status": "success", "message": "Successfully generated and saved key for ccqHub root access.", "payload": fullKey}
    except Exception as e:
        return {"status": "error", "payload": {"error": "There was a problem generating the key for ccqHub root access.", "traceback": traceback.format_exc(e)}}


def encryptString(data):
    # Perform the actual encryption of the data utilizing the key that is provided
    try:
        from cryptography.fernet import Fernet

        values = retrieveEncryptionKey()
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}
        else:
            key = values['payload']

        f = Fernet(key)
        encData = f.encrypt(str(data))
        return {"status": "success", "payload": encData}
    except Exception as e:
        return {"status": "error", "payload": {"error": "There was a problem encrypting the string.", "traceback": str(traceback.format_exc(e))}}


def generateEncryptionKey():
    # Generate the key to be used for encryption if there is not one created. This will be placed in a file that is only
    # accessible by the administrator of ccqHub and therefore cannot be run by other users since they cannot read the file
    try:
        from cryptography.fernet import Fernet
        key = Fernet.generate_key()

        # Now that the key has been generated we need to store it in a file in a location that is only accessible by the
        # admin ccqHub user

        if not os.path.isdir(str(ccqHubVars.ccqHubPrefix) + str(ccqHubKeyFile)):
            status, output = commands.getstatusoutput("mkdir " + str(ccqHubVars.ccqHubPrefix) + str(ccqHubKeyDir))
            if int(status) != 0:
                # Creation of the directory failed print the output and exit
                print "There was an error trying to create the ccqHub key directory. The error message is: " + str(output)
                sys.exit(0)

        try:
            keyFile = open(str(ccqHubVars.ccqHubPrefix) + str(ccqHubKeyFile), "w+")
            keyFile.write("Warning changing the contents of this file may render ccqHub data inaccessible.\n")
            keyFile.write(str(key))
            keyFile.close()

            status, output = commands.getstatusoutput("chmod -R 400 " + str(ccqHubVars.ccqHubPrefix) + str(ccqHubKeyDir))
            if int(status) != 0:
                # Creation of the directory failed print the output and exit
                print "There was an error trying to create the ccqHub key directory. The error message is: " + str(output)
                sys.exit(0)

        except Exception as e:
            return {"status": "error", "payload": {"error": "There was an issue writing out the keyfile.", "traceback": str(traceback.format_exc(e))}}
        return {"status": "success", "payload": key}
    except ImportError as ie:
        return {"status": "error", "payload": {"error": "ccqHub requires the pip package cryptography to be installed in order to function properly. Please install the cryptography pip package and try installing ccqHub again.", "traceback": str(traceback.format_exc(ie))}}
    except Exception as e:
        return {"status": "error", "payload": {"error": "There was a problem generating the encryption key for ccqHub.", "traceback": str(traceback.format_exc(e))}}


def retrieveEncryptionKey():
    try:
        keyFile = open(str(ccqHubVars.ccqHubPrefix) + str(ccqHubKeyFile), "r")

        # Read the comment line and throw it out
        keyFile.readline()
        # Read the line with the key in it and save it to a variable to be returned
        keyLine = keyFile.readline()
        keyFile.close()
        return {"status": "success", "payload": keyLine}
    except Exception as e:
        return {"status": "error", "payload": {"error": "There was a problem trying to obtain the key from the specified file.", "traceback": str(traceback.format_exc(e))}}


def decryptString(data):
    try:
        from cryptography.fernet import Fernet

        values = retrieveEncryptionKey()
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}
        else:
            key = values['payload']
            f = Fernet(key)
            decData = f.decrypt(str(data))
            return {"status": "success", "payload": decData}
    except Exception as e:
        return {"status": "error", "payload": {"error": "There was a problem decrypting the string.", "traceback": str(traceback.format_exc(e))}}


def validateAppKey(ccAccessKey):
    identityUuid = None
    response = queryObj(None, "RecType-Identity-keyId-" + str(ccAccessKey.split(":")[0]) + "-name-", "query", "json", "beginsWith")
    if response['status'] == "success":
        results = response['payload']
        for tempItem in results:
            identityUuid = tempItem['name']
            try:
                # Need to load the list of keys from the user and decrypt the object
                results = decryptString(tempItem['keyInfo'])
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
                #certDecodedPass = decryptString(tempItem['password'])
                #if certDecodedPass['status'] != "success":
                #    return {"status": "error", "payload": "App Key not valid."}
                #else:
                #    certDecodedPass = certDecodedPass['payload']
                #    certDecodedUser = tempItem['userName']
                return {"status": "success", "payload": {"message": "Successfully validated the key.", "identity": str(identityUuid)}}
            else:
                #The AccessKey provided is not valid return error
                return {"status": "error", "payload": "App Key not valid."}

        #If the APIKey object isn't found in the DB return not valid
        return {"status": "error", "payload": "App Key not valid."}

    else:
        #If the APIKey object isn't found in the DB return not valid
        return {"status": "error", "payload": "App Key not valid."}


def encodeString(k, field):
    enchars = []
    for i in xrange(len(field)):
        k_c = k[i % len(k)]
        enc = chr(ord(field[i]) + ord(k_c) % 256)
        enchars.append(enc)
    ens = "".join(enchars)
    return base64.urlsafe_b64encode(ens)

