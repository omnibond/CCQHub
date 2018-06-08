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

import os
import sys
import time

from ccqHubMethods import writeCcqVarsToFile

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
from Slurm import SlurmScheduler
from Torque import TorqueScheduler

import urllib2
import json
import traceback
import commands
from datetime import datetime
from datetime import timedelta
import platform
import ccqHubMethods
import threading
import ccqHubVars
import paramiko
import credentials
import logging

tempJobScriptLocation = ""#ClusterMethods.tempScriptJobLocation
tempJobOutputLocation = ""#ClusterMethods.tempJobOutputLocation
logDirectory = ""#ClusterMethods.logFileDirectory

#####################################
#  Possible CCQHub States and meanings #
#####################################
# Pending - The job has been submitted via ccqsub and is in the DB but has not yet been processed by ccqHubHandler.py
# Processing - ccqHubHandler is applying the rules and performing the routing of the job
# Queued - ccqHubHandler is waiting to send the job to the scheduler
# Submitted - ccqHubHandler has sent the job to scheduler and it is now up to the scheduler to handle the execution of the job

# Can have multiple states now depending on the scheduler:
# Pending/Queued/CCQueued/Provisioning/Running/Completed/Error etc. These states will come directly from the remote scheduler

# Completed - the job has finished and the remote scheduler has marked the job as completed. ccqHubHandler will now work to transfer the output files and perform other post processing tasks

# deleting - the job has been deleted via ccqdel and no other actions will be performed on the job. If the job was in the process of creating a compute group or expanding a compute group the creation and expansion will proceed

# Error - ccqHubHandler encountered an error while attempting to run the job. These errors will be logged in a TBD place


def performJobRouting(logger):
    logger.info("Placeholder for actual rules/routing policy evaluation and enforcement.")


def createSchedulerObject(schedName, schedType, schedulerInstanceId, schedulerHostName, schedulerIpAddress, clusterName):
    # Create the scheduler object that will be used to call the scheduler specific methods
    kwargs = {"schedName": schedName, "schedType": schedType, "instanceID": schedulerInstanceId,  "clusterName": clusterName, "instanceName": schedulerHostName, "schedulerIP": schedulerIpAddress}
    try:
        if schedType == "Torque":
            scheduler = TorqueScheduler(**kwargs)
        elif schedType == "Slurm":
            scheduler = SlurmScheduler(**kwargs)
        else:
            return {"status": "error", "payload": "Unable to create the scheduler object, unsupported scheduler type " + str(schedType)}
        return {"status": "success", "payload": scheduler}
    except:
        return {"status": "error", "payload": "Unable to create the scheduler object"}


def cloudJobSubmission(jobObj, logger):
    logger.info("The timestamp when starting submitJob is: " + str(time.time()))
    jobId = jobObj["name"]
    remoteUserName = jobObj["userName"]
    jobMD5Hash = ""
    ccOptionsParsed = []

    jobName = jobObj['jobName']
    jobScriptLocation = jobObj['jobScriptLocation']
    jobScriptText = jobObj['jobScriptText']

    # We need to get the job script object here because we need certain information that we can't get in just the job obj
    results = ccqHubMethods.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict")
    if results['status'] == "success":
        results = results['payload']
    else:
        return {'status': 'error', 'payload': results['payload']}
    for jobScript in results:
        jobMD5Hash = jobScript['jobMD5Hash']
        ccOptionsParsed = json.loads(jobScript['ccOptionsParsed'])

    targetAddresses = json.loads(ccqHubVars.jobMappings[jobId]["targetAddresses"])
    targetProxyKeys = json.loads(ccqHubVars.jobMappings[jobId]["targetProxyKeys"])
    targetProtocol = ccqHubVars.jobMappings[jobId]["targetProtocol"]
    targetAuthType = ccqHubVars.jobMappings[jobId]["targetAuthType"]

    encodedUserName = ""
    encodedPassword = ""
    valKey = ""
    dateExpires = ""
    certLength = 1

    # Need to try and submit to all of the targetAddresses. Once one is successfully submitted we quit. If we get an error in the connection we retry with the other addresses
    submittedSuccessfully = False
    # Need to try and submit using all of the target proxy keys. Once one submits successfully we exit, if there is an error due to the key we retry.
    for address in targetAddresses:
        badURL = False
        if not submittedSuccessfully:
            try:
                for proxyKey in targetProxyKeys:
                    if not badURL:
                        final = {"jobScriptLocation": str(jobScriptLocation), "jobScriptFile": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash, "userName": str(encodedUserName), "password": str(encodedPassword), "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "ccAccessKey": str(proxyKey), "remoteUserName": str(remoteUserName)}
                        data = json.dumps(final)
                        headers = {'Content-Type': "application/json"}
                        url = "https://" + str(address) + "/srv/ccqsub"
                        res = []
                        try:
                            req = urllib2.Request(url, data, headers)
                            res = urllib2.urlopen(req).read().decode('utf-8')
                            print(res)
                            res = json.loads(res)
                        except Exception as e:
                            # We couldn't connect to the url so we need to try the other one.
                            badURL = True
                            logger.info(''.join(traceback.format_exc(e)))
                        if not badURL:
                            if res['status'] == "failure":
                                if proxyKey is not None:
                                    logger.info("The key is not valid, please check your key and try again.")
                            elif res['status'] == "error":
                                #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
                                logger.info(res['payload']['message'] + "\n\n")
                            elif res['status'] == "success":
                                ccqJobId = res['payload']['message'].split(":")[1][1:5]
                                with ccqHubVars.ccqHubVarLock:
                                    ccqHubVars.jobMappings[jobId]['status'] = "ccqHubSubmitted"
                                values = ccqHubMethods.updateJobInDB({"status": "ccqHubSubmitted", "jobIdInCcq": str(ccqJobId)}, jobId)
                                if values['status'] != "success":
                                    return {"status": "error", "payload": values['payload']}
                                logger.info("The job has been successfully submitted.")
                                return {"status": "success", "payload": "Successfully submitted the job to the scheduler"}
            except Exception as e:
                # We encountered an unexpected exception
                logger.info(''.join(traceback.format_exc(e)))
    return {"status": "error", "payload": "Unable to successfully submit the job to the specified scheduler. The key or the target URL is invalid."}


def localJobSubmission(jobObj, logger):
    values = createSchedulerObject(jobObj['targetName'], jobObj['schedType'], None, None, None, None)
    logger.info("Need to do some work here, shouldn't be too bad actually. Just need to modify the Scheduler file to fit the needs of the local scheduler")


#Determine whether the job needs to create resources, wait, or use existing resources. This method determines what happens to the job
def determineNextStepsForJob(jobId, targetName, schedType, logger):
    #If the job has been killed there is no need to process it
    if ccqHubVars.jobMappings[jobId]['status'] == "Killed":
        return {"status": "success", "payload": {"nextStep": "none"}}

    #Check to see if the job is submittable to any of the currently available resources the scheduler has
    #Also checks jobs already set to expand as well in case things have changed since they were assigned a compute group
    logger.info("The job Id is: " + str(jobId))
    #print "ccqHubVars.jobMappings[jobId][isCreating] is: " + str(ccqHubVars.jobMappings[jobId]['isCreating'])
    if ccqHubVars.jobMappings[jobId]['status'] == "Pending":
        # The job is new and has not been processed by ccqHubLauncher yet. We need to send the job off to the appropriate scheduler
        # Need to get target information here so we can decide what to do with the job, is it local or need to be sent to the Cloud
        targetInfo = {}
        values = ccqHubMethods.getTargetInformation(targetName, schedType)
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}
        else:
            targetInfo = values['payload']

        # The scheduler that the job is being submitted to is a Cloud based ccq scheduler so we need the authentication method, the protocol to use, and the DNS name
        targetType = targetInfo['schedulerType']
        targetProtocol = targetInfo['protocol']
        targetAuthType = targetInfo['authType']
        targetProxyKeys = targetInfo['proxyKey']
        targetAddresses = targetInfo['targetAddress']

        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}

        with ccqHubVars.ccqHubVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "Processing"
            ccqHubVars.jobMappings[jobId]["targetType"] = str(targetType)
            ccqHubVars.jobMappings[jobId]["targetProtocol"] = str(targetProtocol)
            ccqHubVars.jobMappings[jobId]["targetAuthType"] = str(targetAuthType)
            ccqHubVars.jobMappings[jobId]["targetProxyKeys"] = targetProxyKeys
            ccqHubVars.jobMappings[jobId]["targetAddresses"] = targetAddresses

        values = ccqHubMethods.updateJobInDB({"status": "Processing", "targetType": str(targetType), "targetProtocol": str(targetProtocol), "targetAuthType": str(targetAuthType), "targetProxyKeys": targetProxyKeys, "targetAddresses": targetAddresses}, jobId)
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}

        # Now we need to determine what the next step for the job is
        if targetType == "ccq":
            # We need to submit the job to the cloud scheduler and start a thread to monitor the progress of the job
            return {"status": "success", "payload": {"nextStep": "cloudSubmit"}}
        else:
            # We need to do a local submit and start a process to monitor the progress of the job
            return {"status": "success", "payload": {"nextStep": "localSubmit"}}

    #TODO need to add the code for the other types of states that the job can go into
    elif ccqHubVars.jobMappings[jobId]['status'] == "ccqHubSubmitted":
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Completed":
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Error":
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Killed":
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Deleting":
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}
    else:
        #The job is still being processed by the scheduler and there is nothing we need to do at this time
        logger.info("Not yet implemented")
        return {"status": "success", "payload": {"nextStep": "notImplemented"}}


def determineJobsToProcess(logger):
    logger.info("Inside of determineJobsToProcess")
    #Check to see which job is next in line to be run
    jobSubmitTimes = {}
    jobs = {}
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        logger.info("Error: QueryErrorException! Unable to get Item!")
        return {'status': 'error', 'payload': results['payload']}
    for job in results:
        logger.info("This is the job[status] " + str(job['status']))
        if job['status'] != "CCQueued" and job['status'] != "Error" and job['status'].lower() != "completed" and str(job['isSubmitted']).lower() == "false":
            jobSubmitTimes[job['name']] = job['dateSubmitted']
            jobs[job['name']] = job

    sortedJobsBySubmissionTime = sorted(jobSubmitTimes.items(), key=lambda x: x[1])
    logger.info("The jobs sorted by submission time are: " + str(sortedJobsBySubmissionTime))

    return {"status": "success", "payload": {"jobs": jobs, "sortedJobs": sortedJobsBySubmissionTime}}


def cleanupDeletedJob(jobId, logger):
    #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it
    logger.info("SOMEHOW MADE IT INTO CLEANUPDELETEDJOB FUNCTION!!!!\n\n\n\n\n")
    with ccqHubVars.ccqHubVarLock:
        ccqHubVars.jobMappings.pop(jobId)
    writeCcqVarsToFile()


def monitorJobs(logger, schedulerType):
    #Run for the duration of the ccqLauncher and monitor the jobs and instances for things that need deleted
    while True:
        try:
            time.sleep(30)
            if schedulerType == "cloud":
                logger.info("The timestamp when start monitorCloudJobs is: " + str(time.time()))
                monitorCloudJobs(logger)
                logger.info("The timestamp when end monitorCloudJobs is: " + str(time.time()))
            elif schedulerType == "local":
                logger.info("The timestamp when start monitorLocalJobs is: " + str(time.time()))
                monitorLocalJobs(logger)
                logger.info("The timestamp when end monitorLocalJobs is: " + str(time.time()))
        except Exception as e:
            logger.info("There was an error encountered in monitorJobs:\n")
            logger.info(traceback.format_exc(e))


def monitorLocalJobs(logger):
    #TODO put stuff to monitor local jobs here.
    logger.info("Put stuff to monitor local jobs here.")


def monitorCloudJobs(logger):
    #Get the jobs from the list of job mappings maintained by ccqLauncher
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
    if results['status'] == "success":
        jobsToCheck = results['payload']
    else:
        logger.info("Error: QueryErrorException! Unable to get Item!")
        return {'status': 'error', 'payload': results['payload']}
    try:
        # Loop through all of the jobs and update them accordingly. If the job is being processed by ccq in the cloud we need to update the status of it to correlate with the status in remote ccq. If it is completed we need to see if the job has been sitting in the DB for more then 1 day and if so we delete it.
        for job in jobsToCheck:
            #Check to ensure that the job has finished creating and if so proceed to check if the job has finished running or not and make sure it is not in the Error state or has already been marked as Completed
            logger.info("NOW CHECKING JOB: " + str(job) + " at the start of monitor jobs\n")
            submittedToScheduler = False

            try:
                # Need to see if the job has been submitted to ccq in the cloud. If it has then we move on if it hasn't we return the non-verbose status and move on.
                jobIdInCcq = job['jobIdInCcq']
                submittedToScheduler = True
            except Exception as e:
                # The job has not yet been submitted to the remote scheduler by ccqHubHandler. We do not have anything to update so for now we pass
                pass

            if submittedToScheduler:
                remoteUserName = job['userName']

                targetAddresses = json.loads(job["targetAddresses"])
                targetProxyKeys = json.loads(job["targetProxyKeys"])
                targetProtocol = job["targetProtocol"]
                targetAuthType = job["targetAuthType"]

                valKey = "unpw"
                dateExpires = ""
                encodedUserName = ""
                encodedPassword = ""
                isCert = None
                certLength = 0
                dataRetrieved = False
                # Need to try all of the target proxy keys. Once one has successfully returned we exit, if there is an error due to the key we retry.
                for address in targetAddresses:
                    badURL = False
                    if not dataRetrieved:
                        try:
                            for proxyKey in targetProxyKeys:
                                if not badURL:
                                    url = "https://" + str(address) + "/srv/ccqstat"
                                    final = {"jobId": str(jobIdInCcq), "userName": str(encodedUserName), "password": str(encodedPassword), "verbose": False, "instanceId": None, "jobNameInScheduler": None, "schedulerName": None, 'schedulerType': None, 'schedulerInstanceId': None, 'schedulerInstanceName': None, 'schedulerInstanceIp': None, 'printErrors': False, "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "jobInfoRequest": False, "ccAccessKey": str(proxyKey), "printOutputLocation": False, "printInstancesForJob": False, "remoteUserName": str(remoteUserName), "databaseInfo": True}
                                    data = json.dumps(final)
                                    headers = {'Content-Type': "application/json"}
                                    try:
                                        req = urllib2.Request(url, data, headers)
                                        res = urllib2.urlopen(req).read().decode('utf-8')
                                        res = json.loads(res)
                                    except Exception as e:
                                        # We couldn't connect to the url so we need to try the other one.
                                        badURL = True
                                        logger.info(''.join(traceback.format_exc(e)))
                                    if not badURL:
                                        if res['status'] == "failure":
                                            if proxyKey is not None:
                                                logger.info("The key is not valid, please check your key and try again.")
                                        elif res['status'] == "error":
                                            #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
                                            logger.info(res['payload']['message'] + "\n\n")
                                        elif res['status'] == "success":
                                            dataRetrieved = True
                                            values = ccqHubMethods.parseCcqStatJobInformation(res['payload']['message'])
                                            databaseInfo = values['payload']
                                            newStatus = ""
                                            # Update the state in the ccqHub Database
                                            if databaseInfo['status'] == "Pending":
                                                newStatus = "RemotePending"
                                            elif databaseInfo['status'] == "CreatingCG" or databaseInfo['status'] == "expandingCG":
                                                newStatus = "AllocatingRemoteResources"
                                            elif databaseInfo['status'] == "Provisioning" or databaseInfo['status'] == "CCQueued":
                                                newStatus = "RemoteProcessing"
                                            elif databaseInfo['status'] == "Submitted":
                                                newStatus = "RemoteSubmitted"
                                            elif databaseInfo['status'] == "Error":
                                                newStatus = "RemoteError"
                                            elif databaseInfo['status'] == "Killed":
                                                newStatus = "RemoteKilled"
                                            else:
                                                newStatus = databaseInfo['status']
                                            values = ccqHubMethods.updateJobInDB({"status": str(newStatus)}, job['name'])
                                            with ccqHubVars.ccqHubVarLock:
                                                ccqHubVars.jobMappings[job['name']]['status'] = str(newStatus)
                                            if values['status'] != "success":
                                                logger.info("Encountered an error while trying to update the status of the job in the ccqHub database with the information received from the remote scheduler.")

                        except Exception as e:
                            # We encountered an unexpected exception
                            logger.info(''.join(traceback.format_exc(e)))
                if not dataRetrieved:
                    logger.info("Unable to successfully get the status of the job(s) specified. Please re-check the credentials and try again.")

            # If the job is in the Completed state, we need to transfer the remote job output files back to the ccqHub server where the job was submitted
            if str(ccqHubVars.jobMappings[job['name']]['status']).lower() == "completed":
                # We need to add this job to the thread pool that handles transferring the job output files back to the ccqHub server
                with ccqHubVars.ccqHubVarLock:
                    ccqHubVars.jobsTransferringOutput.append(job['name'])

    #We have added a new job to the queue so we need to retry
    except Exception as e:
        logger.info(''.join(traceback.format_exc()))
        pass


def checkToSeeIfJobStillRunningOnCluster(job, scheduler, logger):
    try:
        logger.info("NOW CHECKING JOB: " + str(job['name']) + " inside of checkToSeeIfJobStillRunningOnCluster\n")
        checkJobsKwargs = {"job": job}
        results = scheduler.checkJob(**checkJobsKwargs)
        jobInDBStillRunning = results['payload']['jobInDBStillRunning']
        if not jobInDBStillRunning:
            endTime = time.time()

            #Need to update the DB entry for the job to completed and set the timestamp of when the job completed.
            values = ccqHubMethods.updateJobInDB({"status": "Completed", "endTime": str(endTime), "instancesRunningOnIds": [], "instancesRunningOnNames": []}, job['name'])
            if values['status'] != "success":
                logger.info(values['payload'])
            logger.info("Job " + str(job['name'] + " has finished running!"))
            logger.info("Updating the averages in the DB!")

            with ccqHubVars.ccqHubVarLock:
                ccqHubVars.jobMappings[job['name']]['status'] = "Completed"
                ccqHubVars.jobMappings[job['name']]['endTime'] = endTime
                for instance in ccqHubVars.jobMappings[job['name']]['instancesToUse']:
                    ccqHubVars.instanceInformation[instance]['state'] = "Available"
                    if instance not in ccqHubVars.availableInstances:
                        ccqHubVars.availableInstances.append(instance)
                ccqHubVars.jobMappings[job['name']]['instancesToUse'] = []
            writeCcqVarsToFile()

            results = ccqHubMethods.queryObj(None, job['name'], "get", "dict")
            if results['status'] == "success":
                results = results['payload']
            else:
                return {"status": "error", "payload": {"error": "Query Error Exception!\n", "traceback": ''.join(traceback.format_stack())}}

            jobDB = {}
            for thing in results:
                jobDB = thing

            transferred = False
            maxTries = 5
            tries = 0
            while not transferred:
                status = copyJobOutputFilesToSpecifiedLocation(jobDB, logger)
                if status['status'] == "success":
                    transferred = True
                else:
                    #If the error contains AuthenticationException: Authentication failed then run the driver.
                    # if scheduler.schedType == "Slurm":
                    #     instanceIP = str(job['batchHost']).split(".")[0].replace("-", ".").replace("ip.", "")
                    #     print "FAILED TO TRANSFER THE OUTPUT FILES, RUNNING USER AND GROUPS COMMANDS AGAIN ON INSTANCE: " + str(instanceIP)
                    #     print "RUNNING COMMAND: salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.groupsAdded"
                    #     print "Unable to verify that the user is on the instance: " + str(instanceIP) + " re-running the group and user states for " + str(instanceIP) + " instance."
                    #     ga = commands.getstatusoutput("salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.groupsAdded")
                    #     print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ga)
                    #     print "RUNNING COMMAND: salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.usersAdded"
                    #     ua = commands.getstatusoutput("salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.usersAdded")
                    #     print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ua)
                    if tries > maxTries:
                        transferred = True
                        #ClusterMethods.writeToErrorLog({"messages": ["There was a problem transferring the output files from the job to the requested location. Please check the " + str(ClusterMethods.tempJobOutputLocation) + str(jobDB['userName']) + " on " + str(scheduler.schedName) + " to see if your output files have been transferred there\n"], "traceback": ["Job Output Transfer Error"]}, "ccq Job " + str(jobDB['name']))
                    tries += 1

            values = ccqHubMethods.calculateAvgRunTimeAndUpdateDB(jobDB['startTime'], str(endTime), jobDB['instanceType'], jobDB['jobName'])
            if values['status'] != "success":
                logger.info(values['payload'])
                logger.info("There was an error calculating the run time for the job!")

        return {"status": "success", "payload": "Finished checking if the job is still running and if not and instances were close to their hour limit and not running other jobs, they were terminated."}
    except Exception as e:
        logger.info(traceback.format_exc(e))
        return {'status': 'error', 'payload': {"error": "Error: "+str(e), "traceback": ''.join(traceback.format_exc(e))}}


def copyJobOutputFilesToSpecifiedLocation(job, logger):
    storedPassword = ""
    accessInstanceName = ""
    res = ccqHubMethods.queryObj(None, "RecType-Collaborator-userName-" + str(job['userName']), "query", "json")
    if res['status'] == "success":
        results = res['payload']
        for item in results:
            res = credentials.decryptString(item['password'])
            if res['status'] == "success":
                storedPassword = res['payload']['string']
            else:
                logger.info("Unable to decrypt the password from the Collaborator!")
            break

    try:
        batchHost = job['batchHost']
        if batchHost is not None:
            #Slurm does not copy the output files to where the job was submitted, but since Torque does this, we need to
            #keep the behavior similar between the two so we need to ssh into the batch host node and get the output files
            #for the job and transfer them to the scheduler.
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(str(batchHost), username=job['userName'], password=str(storedPassword), timeout=15)
                sftp = ssh.open_sftp()
                #Copy the job from the stdoutFileLocation and stderrFileLocation to the standard location used by ccq for
                #transfer to the directory the user called ccqsub from.
                sftp.get(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
                sftp.get(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e")
                sftp.close()
                ssh.close()
            except Exception as e:
                logger.info("Unable to transfer the output files from " + str(batchHost) + "! Due to the following exception: ")
                logger.info(traceback.format_exc(e))
                logger.info("\n\n")
                return {"status": "error", "payload": "There was an error transferring the job output files."}

    except KeyError as e:
        pass

    #By default the files get transferred into the directory where the job was submitted if no special output location is specified
    #Make sure the file has been transferred before attempting to copy it
    time.sleep(30)
    #If the job was submitted remotely (not from a CC instance) then the job output will be transferred to the user's
    #home directory on the login instance for easy access and storage"
    if job['isRemoteSubmit'] == "True":
        #Get Access Instance IP Address:
        results = ccqHubMethods.queryObj(None, "RecType-WebDav-clusterName-" + str(job['schedClusterName']), "query", "dict", "beginsWith")
        if results['status'] == "success":
            results = results['payload']
        else:
            logger.info("Error trying to obtain the Access Instance to transfer the files from!")
            return {"status": "error", "payload": "Error trying to obtain the Access Instance to transfer the files from."}

        for item in results:
            accessInstanceName = item["accessName"]

        if accessInstanceName != "":
            ccqHubMethods.updateJobInDB({"accessInstanceResultsStoredOn": str(accessInstanceName)}, job['name'])
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(str(accessInstanceName), username=job['userName'], password=str(storedPassword), timeout=15)
                sftp = ssh.open_sftp()
                sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", "/home/" + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
                sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", "/home/" + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e")
                sftp.close()
                ssh.close()
                #os.system("rm -rf " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
            except Exception as e:
                logger.info("Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(accessInstanceName) + " ! Due to the following exception: ")
                logger.info(traceback.format_exc(e))
                logger.info("\n\n")
                return {"status": "error", "payload": "There was an error transferring the job output files."}
        else:
            logger.info("Unable to get an Access Instance to copy the results too! Results will stay on the scheduler where the job was submitted!")
    else:
        #Use the submitHostInstanceId parameter to get the IP of the instance to send the information too!
        ipToSendTo = ""
        items = ccqHubMethods.queryObj(None, str(job['submitHostInstanceId']), "get", "dict")
        if items['status'] == "success":
            items = items['payload']

        for instance in items:
            ipToSendTo = instance['instanceIP']
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(str(ipToSendTo), username=job['userName'], password=str(storedPassword), timeout=15)
            sftp = ssh.open_sftp()

            if str(job["stdoutFileLocation"]) == "default":
                stdoutFileLocation = str(job['jobWorkDir']) + "/" + str(job['jobName']) + str(job['name']) + ".o"
            else:
                stdoutFileLocation = job["stdoutFileLocation"]
            if str(job["stderrFileLocation"]) == "default":
                stderrFileLocation = str(job['jobWorkDir']) + "/" + str(job['jobName']) + str(job['name']) + ".e"
            else:
                stderrFileLocation = job["stderrFileLocation"]

            #Put the job output files in the directory where the ccqsub command was ran from on the instance that invoked the call
            sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", stdoutFileLocation)
            sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", stderrFileLocation)
            sftp.close()
            ssh.close()
            os.system("rm -rf " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
            return {"status": "success", "payload": "Output files successfully transferred."}
        except Exception as e:
            logger.info("Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(ipToSendTo) + " ! Due to the following exception: ")
            logger.info(traceback.format_exc(e))
            logger.info("\n\n")
            return {"status": "error", "payload": "There was an error transferring the job output files."}


def delegateTasks(logger):
    logger.info("The timestamp when starting delegateTasks is: " + str(time.time()))
    listOfJobsToProcess = {}
    isStartup = True
    while True:
            #If there are fewer than 10 jobs in the queue wait 20 seconds before attempting to process the next job
            #This prevents spamming the database with queries while the initial instances are creating and allows instances
            #to spin up properly
            if len(listOfJobsToProcess) < 10 and not isStartup:
                time.sleep(20)
            results = determineJobsToProcess(logger)
            isStartup = False
            if results['status'] != "success":
                time.sleep(60)
                logger.info("Failed to find any jobs, sleeping for 20 seconds and checking for jobs again.")
            else:
                listOfJobsToProcess = results['payload']['jobs']

                #Process all of the jobs in the list in the order they were submitted
                for job in results['payload']['sortedJobs']:
                    try:
                        # Get job name out of the sorted item
                        currentJob = listOfJobsToProcess[job[0]]
                        # Make sure that the job is in the ccqHubVars.jobMappings list and if it isn't then add it to the list
                        try:
                            ccqHubVars.jobMappings[currentJob['name']]
                            if currentJob['status'] == "Killed":
                                ccqHubVars.jobMappings[currentJob['name']]['status'] = currentJob['status']
                        except:
                            with ccqHubVars.ccqHubVarLock:
                                ccqHubVars.jobMappings[currentJob['name']] = currentJob

                        logger.info("THE CURRENT JOB IS: " + str(currentJob['name']))

                        # Now that we have the job we need to determine what the next step is for the job
                        values = determineNextStepsForJob(currentJob['name'], currentJob['targetName'], currentJob['schedType'], logger)
                        logger.info(values)
                        if values['status'] != "success":
                            if values['status'] == "error":
                                #ClusterMethods.writeToErrorLog(values['payload'], "ccq Job " + str(currentJob['name']))
                                logger.info(values['payload'])
                            elif values['status'] == "Deleting":
                                #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it also need to add the instances assigned to the job (if any) back to the available instance pool
                                cleanupDeletedJob(currentJob['name'], logger)
                        else:
                            if currentJob['status'] == "Deleting":
                                # The job has been marked for deletion and should be deleted
                                results = ccqHubMethods.handleObj("delete", currentJob)
                                if results['status'] != "success":
                                    logger.info("There was an error trying to delete the job (" + str(currentJob['name']) + ")  that was marked for deletion.")
                                logger.info("The job (" + str(currentJob['name']) + ") has been marked for deletion and has been successfully deleted from the ccqHub Database.")
                                with ccqHubVars.ccqHubVarLock:
                                    ccqHubVars.jobMappings.pop(currentJob['name'])
                            else:
                                logger.info(values['payload']['nextStep'])
                                if values['payload']['nextStep'] == "cloudSubmit":
                                    values = cloudJobSubmission(currentJob, logger)
                                    logger.info(values)

                                elif values['payload']['nextStep'] == "localSubmit":
                                    # TODO do stuff here for the local job submission implementation
                                    values = localJobSubmission(currentJob, logger)
                                    logger.info(values)
                                else:
                                    logger.info("The nextStep state requested has not been implemented yet.")
                    except Exception as e:
                        logger.info("Encountered a breaking error on job " + str(listOfJobsToProcess[job[0]]))
                        logger.info(traceback.format_exc())
                    logger.info("The time at the end of delegate tasks is: " + str(time.time()))


def createLoggerForThread(loggerName, loggerLevel, loggerFormatter, loggerOutputFileName, displayInConsole, useSTDIN):
    # Log file/logger interface for the monitorJobs thread
    logger = logging.getLogger(str(loggerName))
    if str(loggerLevel).lower() == "debug":
        logger.setLevel(logging.DEBUG)

    # create a file handler writing to a file named after the thread
    file_handler = logging.FileHandler(str(loggerOutputFileName))

    if loggerFormatter is not None:
        # create a custom formatter and register it for the file handler
        formatter = logging.Formatter(str(loggerFormatter))
        file_handler.setFormatter(formatter)

    if displayInConsole:
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)

    if useSTDIN:
        logging.StreamHandler(sys.stdout)

    # register the file handler for the thread-specific logger
    logger.addHandler(file_handler)

    return logger


def main():
        # Get the prefix to where ccqHub will be running from so that we can create the log files there
        prefix = sys.argv[1] + "/logs/"
        #Set the global variables that ccq will use throughout its operation
        ccqHubVars.init()
        print("Successfully initialized the global variables for ccq.")

        #Set the locks for the threads to ensure data integrity and to ensure multiple threads do not write to the same ccq variable or ccq status file at once
        ccqHubVars.ccqHubVarLock = threading.RLock()
        ccqHubVars.ccqHubFileLock = threading.RLock()
        ccqHubVars.ccqHubDBLock = threading.RLock()

        # Create the logger for the monitorJobsThread
        monitorCloudJobsThreadLogger = createLoggerForThread("monitorCloudJobsThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_Monitor_Cloud_Jobs.log", False, False)
        monitorJobsLocalThreadLogger = createLoggerForThread("monitorLocalJobsThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_Monitor_Local_Jobs.log", False, False)

        # Create the delegateTasks logger
        delegateTasksThreadLogger = createLoggerForThread("delegateTasksThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_Delegate_Tasks.log", False, False)

        cloudJobMonitorThread = threading.Thread(target=monitorJobs, args=[monitorCloudJobsThreadLogger, "cloud"])
        cloudJobMonitorThread.start()

        localJobMonitorThread = threading.Thread(target=monitorJobs, args=[monitorJobsLocalThreadLogger, "local"])
        localJobMonitorThread.start()
        #print "Successfully started the monitorJobs thread to check and monitor the jobs."

        ccqDelegateTasksThread = threading.Thread(target=delegateTasks, args=[delegateTasksThreadLogger])
        ccqDelegateTasksThread.start()

        #Run forever and check to make sure the threads are running
        while True:
            time.sleep(120)
            if not ccqDelegateTasksThread.is_alive:
                print("UHOH WE DIED AT SOME POINT")
                ccqDelegateTasksThread.start()
            else:
                print("The ccqDelegateTasksThread is running.")

            if not cloudJobMonitorThread.is_alive:
                print("UHOH WE DIED AT SOME POINT")
                cloudJobMonitorThread.start()
            else:
                print("The cloudJobMonitorThread is running.")

            if not localJobMonitorThread.is_alive:
                print("UHOH WE DIED AT SOME POINT")
                localJobMonitorThread.start()
            else:
                print("The localJobMonitorThread is running.")
main()
