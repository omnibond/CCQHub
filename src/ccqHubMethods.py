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
ccqHubKeyFile = "/.keys/ccqHub.key"


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
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

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
        items = dbInterface.queryObj(None, "RecType-JobScript-name-" + str(parameter), "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

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
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for jobId in items:
            if jobId['jobName'] == parameter or jobId['name'] == jobName:
                return {"status": "success", "payload": {"isTaken": True}}
        return {"status": "success", "payload": {"isTaken": False}}

def putJobScriptInDB(obj):
    ccOptionsParsed = obj['ccOptionsParsed']
    jobName = obj['jobName']
    jobScriptText = obj['jobScriptText']
    jobScriptLocation = obj['jobScriptLocation']
    userName = obj['userName']
    jobMD5Hash = obj['jobMD5']

    obj = {}
    obj['action'] = "create"
    data = {'name': str(jobName), 'RecType': 'JobScript', 'schedType': str(ccOptionsParsed['schedType']), 'jobScriptText': str(jobScriptText),
            'jobScriptLocation': str(jobScriptLocation), "dateFirstSubmitted": str(time.time()), "runTimes": {}, "numberOfTimesRun": str("0"), "createdByUser": str(userName), "jobMD5Hash": str(jobMD5Hash)}
    for command in ccOptionsParsed:
        if ccOptionsParsed[command] != "None":
            data[command] = ccOptionsParsed[command]
    obj['obj'] = data
    response = dbInterface.handleObj(obj)
    if response['status'] == "success" or response['status'] == "partial":
        item = response['payload']
        return {"status": "success", "payload": "Successfully saved the job script to the database!"}
    else:
        return {"status": "error", "payload": str(response['payload'])}

def putJobToRunInDB(obj):
    ccOptionsParsed = obj['ccOptionsCommandLine']
    jobName = obj['jobName']
    jobScriptText = obj['jobScriptText']
    jobScriptLocation = obj['jobScriptLocation']
    userName = obj['userName']
    schedulerToUse = ccOptionsParsed["schedulerToUse"]
    instanceType = ccOptionsParsed["instanceType"]
    schedulerIP = obj['schedulerIP']

    isRemoteSubmit = obj['isRemoteSubmit']

    stdoutFileLocation = ccOptionsParsed["stdoutFileLocation"]
    stderrFileLocation = ccOptionsParsed["stderrFileLocation"]
    jobWorkDir = ccOptionsParsed["jobWorkDir"]

    submitHostInstanceId = obj['submitHostInstanceId']
    schedClusterName = obj['schedClusterName']

    generatedJobId = ""
    done = False
    while not done:
        jobNums = [randint(0,9) for p in range(0,4)]
        for digit in jobNums:
          generatedJobId +=str(digit)

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

    obj = {'action': "create"}
    data = {'name': str(generatedJobId), "jobName": str(jobName), 'RecType': 'Job', "schedulerUsed": str(schedulerToUse), 'schedType': str(ccOptionsParsed['schedType']), 'jobScriptText': str(jobScriptText),'jobScriptLocation': str(jobScriptLocation), "dateSubmitted": str(time.time()), "startTime": time.time(), "instanceType": str(instanceType), "userName": str(userName), "schedulerIP": str(schedulerIP), "stderrFileLocation": str(stderrFileLocation) , "stdoutFileLocation": str(stdoutFileLocation), "submitHostInstanceId": str(submitHostInstanceId), "schedClusterName": str(schedClusterName), "isRemoteSubmit": str(isRemoteSubmit), "jobWorkDir": str(jobWorkDir)}
    for command in ccOptionsParsed:
        if ccOptionsParsed[command] != "None":
            data[command] = ccOptionsParsed[command]
    obj['obj'] = data
    response = dbInterface.handleObj(obj)
    if response['status'] == "success" or response['status'] == "partial":
        item = response['payload']

        done = False
        timeToWait = 10
        maxTimeToWait = 120
        timeElapsed = 0
        while not done:
            try:
                items = dbInterface.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
                if items['status'] == "success":
                    items = items['payload']
                else:
                    print "Error: QueryErrorException! Unable to get Item!"
                    return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

                for jobScript in items:
                    jobScript['numberOfTimesRun'] = int(jobScript['numberOfTimesRun'])+1
                    jobScript.save()
                done = True
            except Exception as e:
                if timeElapsed >= maxTimeToWait:
                    return {"status": "error", "payload": "Failed to update number of times the job has run!"}
                time.sleep(timeToWait)
                timeElapsed += timeToWait

        return {"status": "success", "payload": {"jobId": str(generatedJobId)}}
    else:
        return {"status": "error", "payload": {str(response['payload'])}}

def getSchedulerIPInformation(schedName, schedType):
    if schedName == "default":
        items = dbInterface.queryObj(None, "RecType-Scheduler-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            if schedType.lower() == str(scheduler['schedType']).lower() and scheduler['defaultScheduler'] == "true":
                isAutoscaling = False
                if scheduler['scalingType'] == "autoscaling":
                    isAutoscaling = True
                return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP']), "clusterName": str(scheduler['clusterName']), "schedulerType": str(scheduler['schedType']), "schedName": str(scheduler['schedName']), "isAutoscaling": isAutoscaling, "instanceName": scheduler['instanceName'], "isDefaultScheduler": scheduler['defaultScheduler'], "schedulerInstanceId": scheduler["instanceID"]}}

    items = dbInterface.queryObj(None, "RecType-Scheduler-schedName-" + str(schedName) + "-", "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for scheduler in items:
        if schedType.lower() == "default" or schedType.lower() == str(scheduler['schedType']).lower():
            isAutoscaling = False
            if scheduler['scalingType'] == "autoscaling":
                isAutoscaling = True
            return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP']), "clusterName": str(scheduler['clusterName']), "schedulerType": str(scheduler['schedType']), "schedName": str(scheduler['schedName']), "isAutoscaling": isAutoscaling, "instanceName": scheduler['instanceName'], "isDefaultScheduler": scheduler['defaultScheduler'], "schedulerInstanceId": scheduler["instanceID"]}}

    #Need to eventually figure out what the default Scheduler for the different types are and if a Scheduler Name isn't
    #specified then we will choose the default Scheduler for that certain type of job

    return {'status': 'error', 'payload': "The requested scheduler was not found in the Database!"}

# This method will only be called when we are submitting locally and therefore shouldn't need to do any of the cert auth
# stuff because it will be being called locally only.
def readyJobForScheduler(obj):
    jobInDBAlready = False
    jobScriptLocation = obj['jobScriptLocation']
    jobScriptText = obj['jobScriptFile']
    ccOptionsCommandLine = obj['ccOptionsCommandLine']
    jobName = obj["jobName"]
    jobMD5Hash = obj["jobMD5Hash"]
    userName = obj["userName"]
    password = obj["password"]
    schedulerToUse = ccOptionsCommandLine["schedulerToUse"]
    schedType = ccOptionsCommandLine["schedType"]
    isRemoteSubmit = obj["isRemoteSubmit"]

    #TODO Will need to obtain username and password here!
    decodedUserName = None
    decodedPassword = None

    schedName = schedulerToUse

    #get the instance ID
    urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id')
    instanceId = urlResponse.read()

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        isAutoscaling = values['payload']['isAutoscaling']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}
    #print schedulerIpAddress
    #print clusterName

    obj = {"clusterName": str(clusterName), "ccOptionsParsed": ccOptionsCommandLine}
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
        obj = {"clusterName": str(clusterName), "jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsParsed": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(decodedUserName)}
        values = putJobScriptInDB(obj)
        if values['status'] == 'success':
            print values['payload']
        else:
            return {"status": "error", "payload": values['payload']}

    #Save the running job to the DB
    newObj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(decodedUserName), "password": str(decodedPassword), "schedulerIP": str(schedulerIpAddress), "isRemoteSubmit": str(isRemoteSubmit), "submitHostInstanceId": str(instanceId), "schedClusterName": str(clusterName)}
    values = putJobToRunInDB(newObj)
    if values['status'] != 'success':
        return {"status": "error", "payload": values['payload']}
    else:
        jobId = values['payload']['jobId']

    return {"status": "success", "payload": "The job has successfully been submitted to the scheduler " + str(schedName) + " and is currently being processed! The job id is: " + str(jobId) + " you can use this id to look up the job status using the ccqstat utility."}

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

def encodeString(k, field):
    enchars = []
    for i in xrange(len(field)):
        k_c = k[i % len(k)]
        enc = chr(ord(field[i]) + ord(k_c) % 256)
        enchars.append(enc)
    ens = "".join(enchars)
    return base64.urlsafe_b64encode(ens)

def saveAndGenNewUserKey(actions):
    import hashlib
    import binascii
    import uuid
    try:
        dk = hashlib.pbkdf2_hmac('sha256', str(uuid.uuid4()), os.urandom(128), 100000)
        #Add the newly generated key and the key's permissions to the key object
        #TODO in the future we may add the ability to create ccqHub Users and give them permissions within ccqHub
        identityUuid = str(uuid.uuid4())
        key = binascii.hexlify(dk)

        obj = {'action': "create", 'obj': {"RecType": "Identity", "name": str(identityUuid), "userName": [], "key": [str(key)]}}

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
            return {"status": "success", "message": "Successfully generated and saved key for ccqHub root access.", "payload": binascii.hexlify(dk)}
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
