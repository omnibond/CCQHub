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
import pytz
import paramiko
import credentials

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


def performJobRouting():
    print "Placeholder for actual rules/routing policy evaluation and enforcement."

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


def jobSubmission(jobObj, ccAccessKey):
    print "The timestamp when starting submitJob is: " + str(time.time())
    jobScriptText = jobObj['jobScriptText']
    jobId = jobObj["name"]
    jobName = jobObj['jobName']
    userName = jobObj["userName"]
    jobMD5Hash = jobObj['jobMD5Hash']
    password = ""
    submitHost = ""
    jobScriptLocation = ""
    ccOptionsParsed = ""
    certLength = ""
    dateExpires = ""

    numCpusRequested = jobObj["numCpusRequested"]
    memoryRequested = jobObj["memoryRequested"]
    jobWorkDir = jobObj["jobWorkDir"]

    with ccqHubVars.ccqVarLock:
        ccqHubVars.jobMappings[jobId]['status'] = "Provisioning"
        instancesToUse = ccqHubVars.jobMappings[jobId]['instancesToUse']
    writeCcqVarsToFile()

    encodedPassword = ccqHubMethods.encodeString("ccqpwdfrval", str(password))
    encodedUserName = ccqHubMethods.encodeString("ccqunfrval", str(userName))
    valKey = "unpw"
    isCert = ""
    attempts =0

    url = "https://" + str(submitHost) + "/srv/ccqsub"
    final = {"jobScriptLocation": str(jobScriptLocation), "jobScriptFile": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash, "userName": str(encodedUserName), "password": str(encodedPassword), "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "ccAccessKey" : str(ccAccessKey)}
    data = json.dumps(final)
    headers = {'Content-Type': "application/json"}
    req = urllib2.Request(url, data, headers)
    try:
        res = urllib2.urlopen(req).read().decode('utf-8')
        #print res
        res = json.loads(res)
        if res['status'] == "failure":
            if not isCert and ccAccessKey is None:
                print str(res['payload']['message']) + "\n\n"
                attempts += 1
            elif ccAccessKey is not None:
                print "The key is not valid, please check your key and try again."
                sys.exit(0)
            else:
                isCert = False
                ccAccessKey = None
        elif res['status'] == "error":
            #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
            print res['payload']['message'] + "\n\n"
    except Exception as e:
        print traceback.format_exc(e)
    return {"status": "success", "payload": "Successfully submitted the job to the scheduler"}


#Determine whether the job needs to create resources, wait, or use existing resources. This method determines what happens to the job
def determineNextStepsForJob(jobId, scheduler, clusterName):

    #If the job has been killed there is no need to process it
    if ccqHubVars.jobMappings[jobId]['status'] == "Killed":
        return {"status": "success", "payload": {"nextStep": "none"}}
    #############
    #NO CREATION#
    #############
    #Check to see if the job is submittable to any of the currently available resources the scheduler has
    #Also checks jobs already set to expand as well in case things have changed since they were assigned a compute group
    print "The job Id is: " + str(jobId)
    print "ccqHubVars.jobMappings[jobId][isCreating] is: " + str(ccqHubVars.jobMappings[jobId]['isCreating'])
    if ccqHubVars.jobMappings[jobId]['status'] == "Pending" or ccqHubVars.jobMappings[jobId]['isCreating'] == "false" or ccqHubVars.jobMappings[jobId]['isCreating'] == "none":
        with ccqHubVars.ccqVarLock:
            freeInstances = ccqHubVars.availableInstances
            instanceInfo = ccqHubVars.instanceInformation
        writeCcqVarsToFile()
        #Check if there are enough instances available in the scheduler already
        print "LENGTH OF freeInstances: " + str(len(freeInstances))
        print "NUMBER OF INSTANCES REQUESTED: " + str(ccqHubVars.jobMappings[jobId]['numberOfInstancesRequested'])
        if len(freeInstances) >= int(ccqHubVars.jobMappings[jobId]['numberOfInstancesRequested']):
            instancesMeetingJobRequirements = []
            print "MADE IT INSIDE THE CHECK FOR NUMBER OF FREE INSTANCES"
            if str(ccqHubVars.jobMappings[jobId]['useSpot']) != "yes":
                for instance in freeInstances:
                    if str(instanceInfo[instance]['spot']).lower() != "yes":
                        print "FREE INSTANCE CORES AND MEMORY AND NETWORK TYPE " + str(instanceInfo[instance]['cores']) + " and " + str(instanceInfo[instance]['memory']) + " and " + str(instanceInfo[instance]['networkType'])
                        print "REQUESTED CORES AND MEMORY AND NETWORK TYPE" + str(ccqHubVars.jobMappings[jobId]['cores']) + " and " + str(ccqHubVars.jobMappings[jobId]['memory']) + " and " + str(ccqHubVars.jobMappings[jobId]['networkType'])

                        #Need to convert networkType into an int so it can be compared
                        #TODO MAKE THIS A METHOD THAT CAN BE CALLED IN CLUSTERMETHODS
                        networkTypesInIntegerForm = {"very low": 1, "low": 2, "low to moderate": 3, "moderate": 4, "high": 5, "10gb": 6, "10 gigabit": 6, "20 gigabit": 7, "20gb": 7}
                        try:
                            #Must have at least the same number of cores, amount of memory, and network type the job requested
                            if float(instanceInfo[instance]['cores']) >= float(ccqHubVars.jobMappings[jobId]['cores']) and float(instanceInfo[instance]['memory']) >= float(ccqHubVars.jobMappings[jobId]['memory']) and float(networkTypesInIntegerForm[str(instanceInfo[instance]['networkType'])]) >= float(networkTypesInIntegerForm[ccqHubVars.jobMappings[jobId]['networkType']]):
                                print "CHECKING TO SEE IF THE USER SPECIFIED THE INSTANCE TYPE OR NOT"
                                if ccqHubVars.jobMappings[jobId]['userSpecifiedInstanceType'] == "true":
                                    #Make sure we have instances of the requested type
                                    if str(instanceInfo[instance]['instanceType']) == str(ccqHubVars.jobMappings[jobId]['instanceType']):
                                        print "USER SPECIFIED A INSTANCE TYPE AND WE HAVE THOSE INSTANCES AVAILABLE"
                                        instancesMeetingJobRequirements.append(instance)
                                else:
                                    print "USER DID NOT SPECIFY AN INSTANCE TYPE"
                                    #If the instanceType of the instance is the same as originally requested by the job then we can use it, else check the following if to see if we can use it or not
                                    if str(instanceInfo[instance]['instanceType']) == str(ccqHubVars.jobMappings[jobId]['instanceType']):
                                        instancesMeetingJobRequirements.append(instance)
                                    #Do not allow jobs to run on instances are not similar to the one it originally ran on if it meets network and memory requirements or have an instance type of p* g* x*
                                    elif float(instanceInfo[instance]['cores']) == float(ccqHubVars.jobMappings[jobId]['cores']) and "x" not in str(ccqHubVars.jobMappings[jobId]['instanceType']).split(".")[0] and "p" not in str(ccqHubVars.jobMappings[jobId]['instanceType']).split(".")[0] and "g" not in str(ccqHubVars.jobMappings[jobId]['instanceType']).split(".")[0]:
                                        instancesMeetingJobRequirements.append(instance)
                        except Exception as e:
                            print "There was a problem trying to compare free instances and what the job requested!"
                            print "The instance is: " + str(instance)
                            print "The job is: " + str(jobId)
                            print traceback.format_exc(e)
                            pass
            else:
                #If the job is a spot job then the only way we reuse spot instances is if the instance types are the exact same, no price or core/mem/network checks
                for instance in freeInstances:
                    if str(instanceInfo[instance]['spot']).lower() == "yes":
                        print "FREE SPOT INSTANCE CORES AND MEMORY AND NETWORK TYPE " + str(instanceInfo[instance]['cores']) + " and " + str(instanceInfo[instance]['memory']) + " and " + str(instanceInfo[instance]['networkType'])
                        print "REQUESTED SPOT INSTANCE CORES AND MEMORY AND NETWORK TYPE" + str(ccqHubVars.jobMappings[jobId]['cores']) + " and " + str(ccqHubVars.jobMappings[jobId]['memory']) + " and " + str(ccqHubVars.jobMappings[jobId]['networkType'])

                        if str(instanceInfo[instance]['instanceType']) == str(ccqHubVars.jobMappings[jobId]['instanceType']):
                            instancesMeetingJobRequirements.append(instance)

            print "LENGTH OF instancesMeetingJobRequirements: " + str(len(instancesMeetingJobRequirements))
            print "NUMBER OF INSTANCES REQUESTED: " + str(ccqHubVars.jobMappings[jobId]['numberOfInstancesRequested'])
            if len(instancesMeetingJobRequirements) >= int(ccqHubVars.jobMappings[jobId]['numberOfInstancesRequested']):
                print "INSIDE THE LOOP TO VERIFY WE HAVE ENOUGH INSTANCES MEETING THE REQUIREMENTS"
                #We found enough instances free to run the job, need to update our list and assign the job to the instances found
                with ccqHubVars.ccqVarLock:
                    tempInstanceIds = []
                    tempInstanceNames = []
                    for freeInstance in instancesMeetingJobRequirements:
                        #Remove instances from availableInstances list and set the state to ccqReserved in InstanceInformation list
                        ccqHubVars.availableInstances.remove(freeInstance)
                        ccqHubVars.instanceInformation[freeInstance]['state'] = "ccqReserved"
                        tempInstanceIds.append(ccqHubVars.instanceInformation[freeInstance]['instanceID'])
                        tempInstanceNames.append(freeInstance)
                        if len(tempInstanceNames) == int(ccqHubVars.jobMappings[jobId]['numberOfInstancesRequested']):
                            break
                    ccqHubVars.jobMappings[jobId]['instancesToUse'] = tempInstanceNames
                    ccqHubVars.jobMappings[jobId]['isCreating'] = "completed"
                    #If the job was waiting to expand but has now found instances to use then remove it from the waiting list
                    if ccqHubVars.jobMappings[jobId]['status'] == "ExpandingCG":
                        try:
                            ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[jobId]['instanceType'])][ccqHubVars.jobMappings[jobId]['computeGroup']]['jobsWaitingOnGroup'].remove(jobId)
                        except Exception as e:
                            print "THERE WAS A PROBLEM DELETING THE INSTANCE FROM THE COMPUTE GROUP IT WAS WAITING ON TO EXPAND: " + str(traceback.format_exc(e))
                            pass
                    ccqHubVars.jobMappings[jobId]['status'] = "CCQueued"
                    values = ccqHubMethods.updateJobInDB({"instancesRunningOnNames": tempInstanceNames, "instancesRunningOnIds": tempInstanceIds, "status": "CCQueued"}, jobId)
                    #Check and make sure the job is not set to be deleted if it is, return error and do nothing. If it is not  set for deletion, carry on to submit the job
                    if values['status'] == "deleting":
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings[jobId]['status'] = "deleting"
                        #ClusterMethods.writeToErrorLog({"messages": ["The job was deleted before the job could be submitted successfully\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
                        writeCcqVarsToFile()
                        return {"status": "deleting", "payload": "The job was deleted before the job could be submitted successfully\n"}
                    elif values['status'] != 'success':
                        # Need to update the job status here and somehow notify the user the job has failed
                        #ClusterMethods.writeToErrorLog({"messages": ["Unable to update the job " + str(jobId) + " in the DB\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
                        writeCcqVarsToFile()
                        return {"status": "error", "payload": "Unable to update the job " + str(jobId) + " in the DB when setting the job to submit"}
                    else:
                        writeCcqVarsToFile()
                        return {"status": "success", "payload": {"nextStep": "submitJob"}}

    #Job has finished creating or expanding and is not yet queued for submission, ready the job for submission
    elif ccqHubVars.jobMappings[jobId]['isCreating'] == "completed" and ccqHubVars.jobMappings[jobId]['status'] != "CCQueued":
        print "Made it into the job has finished creating/expanding loop!"
        #If creation of the resources has completed but is not yet in the CCQueued state, put it there
        if ccqHubVars.jobMappings[jobId]['status'] == "ExpandingCG" or ccqHubVars.jobMappings[jobId]['status'] == "CreatingCG":
            print "Should be doing submit stuff here......."
            #Do submit stuff here with the new nodes that should be in the jobMappingsObjects
            with ccqHubVars.ccqVarLock:
                tempInstanceIds = []
                for instance in ccqHubVars.jobMappings[jobId]['instancesToUse']:
                    tempInstanceIds.append(ccqHubVars.instanceInformation[instance]['instanceID'])
                ccqHubVars.jobMappings[jobId]['status'] = "CCQueued"
                values = ccqHubMethods.updateJobInDB({"instancesRunningOnNames": ccqHubVars.jobMappings[jobId]['instancesToUse'], "instancesRunningOnIds": tempInstanceIds, "status": "CCQueued"}, jobId)
                # Check and make sure the job is not set to be deleted if it is, return error and do nothing. If it is not  set for deletion, carry on to submit the job
                if values['status'] == "deleting":
                    print "THERE WAS A PROBLEM SETTING THINGS UP status is DELETING: " + str(values)
                    with ccqHubVars.ccqVarLock:
                        ccqHubVars.jobMappings[jobId]['status'] = "deleting"
                    writeCcqVarsToFile()
                    #ClusterMethods.writeToErrorLog({"messages": ["The job was deleted before the job could expand the compute group to the required size\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
                    return {"status": "deleting", "payload": "The job was deleted before the job could expand the compute group to the required size\n"}
                elif values['status'] != 'success' and values['status'] != "deleting":
                    print "THERE WAS A PROBLEM SETTING THINGS UP status is ERROR: " + str(values)
                    #ClusterMethods.writeToErrorLog({"messages": ["Unable to update the job " + str(jobId) + " in the DB\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
                    writeCcqVarsToFile()
                    return {"status": "error", "payload": "Unable to update the job " + str(jobId) + " in the DB"}
                else:
                    writeCcqVarsToFile()
                    return {"status": "success", "payload": {"nextStep": "submitJob"}}
        else:
            return {"status": "success", "payload": {"nextStep": "none"}}

    #If the job is waiting for the compute group to complete then return CreatingCG status to tell it to wait
    elif ccqHubVars.jobMappings[jobId]['status'] == "CreatingCG":
        return {"status": "success", "payload": {"nextStep": "CreatingCG"}}

    #If the job is waiting for a opening to start Provisioning
    elif ccqHubVars.jobMappings[jobId]['isCreating'] == "completed" and ccqHubVars.jobMappings[jobId]['status'] == "CCQueued":
        return {"status": "success", "payload": {"nextStep": "submitJob"}}

    #If the job has been submitted then do nothing
    elif ccqHubVars.jobMappings[jobId]['isSubmitted']:
        return {"status": "success", "payload": {"nextStep": "none"}}

    # Else the only other state is waitingForExpansionToComplete so we return that the job is still waiting for expansion before submitting
    else:
        return {"status": "success", "payload": {"nextStep": "waitForExpansionToComplete"}}


def determineJobsToProcess():
    print "Inside of determineJobsToProcess"
    #Check to see which job is next in line to be run
    jobSubmitTimes = {}
    jobs = {}
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': results['payload']}
    for job in results:
        print "This is the job[status] " + str(job['status'])
        if job['status'] != "CCQueued" and job['status'] != "Error" and job['status'] != "Deleting" and job['status'] != "deleting" and str(job['isSubmitted']).lower() == "false":
            jobSubmitTimes[job['name']] = job['dateSubmitted']
            jobs[job['name']] = job

    sortedJobsBySubmissionTime = sorted(jobSubmitTimes.items(), key=lambda x: x[1])
    print "The jobs sorted by submission time are: " + str(sortedJobsBySubmissionTime)

    return {"status": "success", "payload": {"jobs": jobs, "sortedJobs": sortedJobsBySubmissionTime}}


def cleanupDeletedJob(jobId):
    #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it
    print "SOMEHOW MADE IT INTO CLEANUPDELETEDJOB FUNCTION!!!!\n\n\n\n\n"
    with ccqHubVars.ccqVarLock:
        action = ccqHubVars.jobMappings[jobId]['status']
        if action == "ExpandingCG":
            ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[jobId]['instanceType'])][ccqHubVars.jobMappings[jobId]['computeGroup']]['jobsWaitingOnGroup'].remove(jobId)
            ccqHubVars.jobMappings.pop(jobId)
        elif action == "CCQueued":
            for freeInstance in ccqHubVars.jobMappings[jobId]['instancesToUse']:
                if freeInstance not in ccqHubVars.availableInstances:
                    ccqHubVars.availableInstances.append(freeInstance)
                    ccqHubVars.instanceInformation[freeInstance]['state'] = "None"
            ccqHubVars.jobMappings.pop(jobId)
    writeCcqVarsToFile()


def monitorJobs(scheduler):
    #Get the jobs from the list of job mappings maintained by ccqLauncher
    with ccqHubVars.ccqVarLock:
        jobsToCheck = ccqHubVars.jobMappings
    writeCcqVarsToFile()
    try:
        for job in jobsToCheck:
            #Check to ensure that the job has finished creating and if so proceed to check if the job has finished running or not and make sure it is not in the Error state or has already been marked as Completed
            print "NOW CHECKING JOB: " + str(job) + " at the start of monitor jobs\n"

            #Check to see if the job is still in the DB if it is not then the user deleted the job through the ccqdel command, we need to delete it from the jobMappings object
            temp = ccqHubMethods.queryObj(None, job, "get", "dict")
            if temp['status'] == "success":
                temp = temp['payload']
            else:
                pass

            found = False
            for DDBItem in temp:
                found = True
            #Need to free the instances belonging to the job and also remove it from any compute groups that are waiting to expand
            if not found:
                with ccqHubVars.ccqVarLock:
                    print "Job " + str(job) + " has been deleted from the DB and is now be deleted from the memory object."
                    jobsToCheck[job]['status'] = "deleting"
                    try:
                        for group in ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']]:
                            if job in ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']][group]['jobsWaitingOnGroup']:
                                ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']][group]['jobsWaitingOnGroup'].remove(job)
                    except Exception as e:
                        print "There was a problem removing the job from the jobsWaitingOnGroup variable."
                        print "Maybe compute group for that instance type didn't exist?" + str(job) + " Not fatal error!"
                        print traceback.format_exc(e)
                    try:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        ccqHubVars.jobMappings[job]['instancesToUse']
                        for instance in ccqHubVars.jobMappings[job]['instancesToUse']:
                            try:
                                ccqHubVars.instanceInformation[instance]['state'] = "Available"
                                if instance not in ccqHubVars.availableInstances:
                                    ccqHubVars.availableInstances.append(instance)
                            except Exception as e:
                                #This instance has been deleted from the scheduler already
                                pass
                        ccqHubVars.jobMappings[job]['instancesToUse'] = []

                        #Remove the job from the in-memory object since it is gone from the DB now
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeCcqVarsToFile()

                    except Exception as e:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        print "There was a problem changing the state of the instances reserved for the job and adding it to the available instance list."
                        print "There were no instances assigned to the job " + str(job) + "yet? Not fatal error!"
                        print traceback.format_exc(e)
                        #Remove the job from the in-memory object since it is gone from the DB now
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeCcqVarsToFile()
            else:
                if jobsToCheck[job]['isCreating'] == "completed" and jobsToCheck[job]['status'] != "Error" and jobsToCheck[job]['status'] != "Completed" and jobsToCheck[job]['status'] != "Killed" and jobsToCheck[job]['status'] != "deleting":
                    if jobsToCheck[job]['status'] == "Submitted" or jobsToCheck[job]['status'] == "Running" or jobsToCheck[job]['status'] == "Queued":
                        print "FOUND A JOB THAT NEEDS TO BE CHECKED! NOW CHECKING THE JOB TO SEE IF IT IS STILL RUNNING!\N"
                        jobsToCheck[job]['name'] = str(job)
                        checkToSeeIfJobStillRunningOnCluster(jobsToCheck[job], scheduler)

                elif jobsToCheck[job]['status'] == "Error" or jobsToCheck[job]['status'] == "Completed" or jobsToCheck[job]['status'] == "Killed" or jobsToCheck[job]['status'] == "deleting":
                    print "NOW CHECKING JOB TO SEE IF IT IS PAST TIME TO DELETE IT FROM THE DB: " + "\n"
                    #Need to see if the job has been in the Error or Completed state for more than 1 day and if it has been longer remove it
                    try:
                        #Check to see if the end time was added to the job or not if it errored it does not have an end time then we make the end time now and go from there
                        endTime = jobsToCheck[job]['endTime']
                    except Exception as e:
                        #There is currently no end time on the instance so we add it to the object
                        endTime = time.time()
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings[job]['endTime'] = endTime
                        writeCcqVarsToFile()

                    try:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        #Check to see if there are any instances remaining in the instancesToUse list for a job in the Error, Completed, or Killed state and if there are set their statuses to Available
                        ccqHubVars.jobMappings[job]['instancesToUse']
                        for instance in ccqHubVars.jobMappings[job]['instancesToUse']:
                            try:
                                ccqHubVars.instanceInformation[instance]['state'] = "Available"
                                if instance not in ccqHubVars.availableInstances:
                                    ccqHubVars.availableInstances.append(instance)
                            except Exception as e:
                                # The instance has been removed from the DB and therefore we should not set the state internally
                                pass
                        ccqHubVars.jobMappings[job]['instancesToUse'] = []
                    except Exception as e:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        print "There was a problem trying to release the instances from the job: " + str(job)
                        print traceback.format_exc(e)

                    if jobsToCheck[job]['status'] == "deleting":
                        try:
                            with ccqHubVars.ccqVarLock:
                                ccqHubVars.jobMappings.pop(job)
                            writeCcqVarsToFile()
                        except Exception as e:
                            pass
                    else:
                        now = time.time()
                        difference = now - float(endTime)
                        difference = timedelta(seconds=difference)
                        hours, remainder = divmod(difference.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)

                        totalMinutes = int(hours)*60 + int(minutes)

                        print "THE TOTAL NUMBER OF MINUTES PASSED SINCE THE JOB COMPLETED/ERRORED/WAS KILLED IS: " + str(totalMinutes) + "\n"
                        #If the jobs have been in the DB more than 1 day after entering the Error or Completed state, delete the job from the DB and from the ccqHubVars objects
                        if int(totalMinutes) > 1440:
                            print "THE TOTAL NUMBER OF MINUTES IS GREATER THAN ONE DAY SO WE ARE DELETING THE JOB FROM THE DB\n"
                            obj = {'action': 'delete', 'obj': job}
                            response = ccqHubMethods.handleObj(obj)
                            if response['status'] != 'success':
                                print "There was an error trying to remove the old Jobs from the DB!"
                            else:
                                try:
                                    with ccqHubVars.ccqVarLock:
                                        ccqHubVars.jobMappings.pop(job)
                                    writeCcqVarsToFile()
                                except Exception as e:
                                    pass
                                print "The job " + str(job) + " was successfully deleted from the DB because it completed running over 1 day ago!"
    #We have added a new job to the queue so we need to retry
    except RuntimeError as e:
        pass


def checkToSeeIfJobStillRunningOnCluster(job, scheduler):
    try:
        print "NOW CHECKING JOB: " + str(job['name']) + " inside of checkToSeeIfJobStillRunningOnCluster\n"
        checkJobsKwargs = {"job": job}
        results = scheduler.checkJob(**checkJobsKwargs)
        jobInDBStillRunning = results['payload']['jobInDBStillRunning']
        if not jobInDBStillRunning:
            endTime = time.time()

            #Need to update the DB entry for the job to completed and set the timestamp of when the job completed.
            values = ccqHubMethods.updateJobInDB({"status": "Completed", "endTime": str(endTime), "instancesRunningOnIds": [], "instancesRunningOnNames": []}, job['name'])
            if values['status'] != "success":
                print values['payload']
            print "Job " + str(job['name'] + " has finished running!")
            print "Updating the averages in the DB!"

            with ccqHubVars.ccqVarLock:
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
                status = copyJobOutputFilesToSpecifiedLocation(jobDB)
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
                print values['payload']
                print "There was an error calculating the run time for the job!"

        return {"status": "success", "payload": "Finished checking if the job is still running and if not and instances were close to their hour limit and not running other jobs, they were terminated."}
    except Exception as e:
        print traceback.format_exc(e)
        return {'status': 'error', 'payload': {"error": "Error: "+str(e), "traceback": ''.join(traceback.format_exc(e))}}


def copyJobOutputFilesToSpecifiedLocation(job):
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
                print "Unable to decrypt the password from the Collaborator!"
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
                print "Unable to transfer the output files from " + str(batchHost) + "! Due to the following exception: "
                print traceback.format_exc(e)
                print "\n\n"
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
            print "Error trying to obtain the Access Instance to transfer the files from!"
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
                print "Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(accessInstanceName) + " ! Due to the following exception: "
                print traceback.format_exc(e)
                print "\n\n"
                return {"status": "error", "payload": "There was an error transferring the job output files."}
        else:
            print "Unable to get an Access Instance to copy the results too! Results will stay on the scheduler where the job was submitted!"
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
            print "Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(ipToSendTo) + " ! Due to the following exception: "
            print traceback.format_exc(e)
            print "\n\n"
            return {"status": "error", "payload": "There was an error transferring the job output files."}


def delegateTasks():
    print "The timestamp when starting delegateTasks is: " + str(time.time())
    listOfJobsToProcess = {}
    isStartup=True
    while True:
            #If there are fewer than 10 jobs in the queue wait two minutes before attempting to process the next job
            #This prevents spamming the database with queries while the initial instances are creating and allows instances
            #to spin up properly
            if len(listOfJobsToProcess) < 10 and not isStartup:
                time.sleep(20)
            results = determineJobsToProcess()
            sys.exit(0)
            isStartup = False
            if results['status'] != "success":
                time.sleep(60)
                print "Failed to find any jobs, sleeping for 2 minutes and checking for jobs again."
            else:
                listOfJobsToProcess = results['payload']['jobs']

                #Process all of the jobs in the list in the order they were submitted
                for job in results['payload']['sortedJobs']:
                    try:
                        #Get job name out of the sorted item
                        currentJob = listOfJobsToProcess[job[0]]
                        print "THE CURRENT JOB IS: " + str(currentJob['name'])

                        # Now that we have the job we need to determine what the next step is for the job
                        values = determineNextStepsForJob(currentJob['name'], scheduler, clusterName)
                        if values['status'] != "success":
                            if values['status'] == "error":
                                #ClusterMethods.writeToErrorLog(values['payload'], "ccq Job " + str(currentJob['name']))
                                print values['payload']
                            elif values['status'] == "deleting":
                                #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it also need to add the instances assigned to the job (if any) back to the available instance pool
                                cleanupDeletedJob(currentJob['name'])
                        else:
                            if values['payload']['nextStep'] == "submitJob":
                                with ccqHubVars.ccqVarLock:
                                    if currentJob['name'] in ccqHubVars.jobsInProvisioningState and ccqHubVars.jobMappings[currentJob['name']]['status'] != "Provisioning" and ccqHubVars.jobMappings[currentJob['name']]['status'] != "CCQueued":
                                        ccqHubVars.jobsInProvisioningState.remove(currentJob['name'])
                                    else:
                                        if len(ccqHubVars.jobsInProvisioningState) < 10:
                                            provisioningThread = threading.Thread(target=jobSubmission, args=(currentJob, scheduler))
                                            provisioningThread.start()
                                            #jobSubmission(currentJob, scheduler)
                                            ccqHubVars.jobsInProvisioningState.append(currentJob['name'])
                                        else:
                                            pass

                                    if values['payload']['nextStep'] == "submitJob":
                                        with ccqHubVars.ccqVarLock:
                                            if currentJob['name'] in ccqHubVars.jobsInProvisioningState and ccqHubVars.jobMappings[currentJob['name']]['status'] != "Provisioning" and ccqHubVars.jobMappings[currentJob['name']]['status'] != "CCQueued":
                                                ccqHubVars.jobsInProvisioningState.remove(currentJob['name'])
                                            else:
                                                if len(ccqHubVars.jobsInProvisioningState) < 10:
                                                    provisioningThread = threading.Thread(target=jobSubmission, args=(currentJob, scheduler))
                                                    provisioningThread.start()
                                                    #jobSubmission(currentJob, scheduler)
                                                    ccqHubVars.jobsInProvisioningState.append(currentJob['name'])
                                                else:
                                                    pass

                                    elif values['payload']['nextStep'] == "expandComputeGroup":
                                        with ccqHubVars.ccqVarLock:
                                            computeGroup = ccqHubVars.jobMappings[currentJob['name']]['computeGroup']
                                            print "The computeGroup for the job is: " + str(computeGroup)

                                        myTurn = False
                                        try:
                                            #Check to see if there is more than one job in the list of waiting compute groups. If there is check if the first one is the current jobId and if so start the expansion
                                            if len(ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[currentJob['name']]['instanceType']][computeGroup]['jobsWaitingOnGroup']) != 0:
                                                if ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['jobsWaitingOnGroup'][0] == str(currentJob['name']) and ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['initStatus'] == "Ready" and ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['ccqAction'] != "expanding":
                                                    print "It is my turn because my job name is at the beginning of the jobsWaitingOnGroup list, and the group is currently not expanding"
                                                    myTurn = True
                                            #Check to see if there are any jobs waiting and if not then it is our turn
                                            elif len(ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[currentJob['name']]['instanceType']][computeGroup]['jobsWaitingOnGroup']) == 0 and ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['initStatus'] == "Ready" and ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['ccqAction'] != "expanding":
                                                print "It is my turn because the length of the jobsWaitingOnGroup array is 0 and the compute group is ready and the compute group is not expanding"
                                                myTurn = True

                                        except Exception as e:
                                            #If we hit an exception print it out and assume it is our turn.
                                            print "Exception checking to see if it is the current jobs turn for expansion"
                                            print traceback.format_exc(e)
                                            myTurn = True

                                        if myTurn:
                                            with ccqHubVars.ccqVarLock:
                                                ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][computeGroup]['ccqAction'] = "expanding"
                                                ccqHubVars.jobMappings[currentJob['name']]['isCreating'] = "true"
                                            writeCcqVarsToFile()
                                            expandCGThread = threading.Thread(target=expandComputeGroup, args=(currentJob, computeGroup, scheduler, clusterName))
                                            expandCGThread.start()
                                        else:
                                            #It's not this job's turn yet, try again next time around
                                            with ccqHubVars.ccqVarLock:
                                                ccqHubVars.jobMappings[currentJob['name']]['isCreating'] = "false"
                                            writeCcqVarsToFile()
                                            pass

                                    elif values['payload']['nextStep'] == "createComputeGroup":
                                        if int(currentJob["numberOfInstancesRequested"]) <= 250:
                                            maxInstances = 250
                                        else:
                                            maxInstances = int(currentJob["numberOfInstancesRequested"])

                                        with ccqHubVars.ccqVarLock:
                                            ccqHubVars.jobMappings[currentJob['name']]['isCreating'] = "true"
                                            groupName = "ccauto-" + str(currentJob['name'])
                                            print "SETTING THE GROUP OBJECT FOR: " + str(groupName) + " BEFORE LAUNCHING THE THREAD!"
                                            try:
                                                ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob["name"]]['instanceType'])]
                                                ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob["name"]]['instanceType'])][str(groupName)] = {'ccqAction': "none", "maxInstances": int(maxInstances), "currentInstances": int(currentJob["numberOfInstancesRequested"]), "initStatus": "Not-Ready", "jobsWaitingOnGroup": []}

                                            except Exception as e:
                                                ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob["name"]]['instanceType'])] = {str(groupName): {'ccqAction': "none", "maxInstances": int(maxInstances), "currentInstances": int(currentJob["numberOfInstancesRequested"]), "initStatus": "Not-Ready", "jobsWaitingOnGroup": []}}
                                        writeCcqVarsToFile()

                                        #Add the compute group being created to the list of CGs in the DB
                                        results = ccqHubMethods.queryObj(None, "ccqCGs-" + str(scheduler.schedName) + "-" + str(clusterName), "get", "dict")
                                        if results['status'] == "success":
                                            results = results['payload']
                                        else:
                                            values = ccqsubMethods.updateJobInDB({"status": "Error"}, currentJob['name'])
                                            with ccqHubVars.ccqVarLock:
                                                ccqHubVars.jobMappings[currentJob['name']]['status'] = "Error"
                                            #ClusterMethods.writeToErrorLog({"messages": [values['payload']['error']], "traceback": [values['payload']['traceback']]}, "ccq Job " + str(currentJob['name']))
                                        writeCcqVarsToFile()
                                        #Put the compute group on the ccqCGs object
                                        for item in results:
                                            computeGroups = json.loads(item['computeGroups'])
                                            computeGroups.append(groupName)
                                            item['computeGroups'] = computeGroups
                                            obj = {'action': 'modify', 'obj': item}
                                            response = ccqHubMethods.handleObj(obj)

                                        newCGThread = threading.Thread(target=launchNewComputeGroupForJob, args=(currentJob, vpcId, scheduler, clusterName))
                                        newCGThread.start()
                                    elif values['payload']['nextStep'] == "none":
                                        print "The job has been submitted and no further action is required."
                                    else:
                                        #If the job is in the waitForExpansionToComplete state
                                        #This job is still waiting to be able to perform it's expansion or submission
                                        #Check to make sure that the job hasn't waited more than 20 minutes to expand, if it has the job is changed to allow it to spin up a new compute group
                                        if values['payload']['nextStep'] == "waitForExpansionToComplete" and ccqHubVars.jobMappings[currentJob['name']]['status'] != "CreatingCG":
                                            timeWaited = float(time.time()) - float(ccqHubVars.jobMappings[currentJob['name']]['expansionStartTime'])
                                            difference = timedelta(seconds=timeWaited)
                                            hours, remainder = divmod(difference.seconds, 3600)
                                            minutes, seconds = divmod(remainder, 60)
                                            totalMinutes = int(hours)*60 + int(minutes)
                                            if totalMinutes > 50:
                                                jobGroup = str(ccqHubVars.jobMappings[currentJob['name']]['computeGroup'])
                                                with ccqHubVars.ccqVarLock:
                                                    ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][jobGroup]['ccqAction'] = "none"
                                                    if str(ccqHubVars.jobMappings[currentJob['name']]) in ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][jobGroup]['jobsWaitingOnGroup']:
                                                        ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[currentJob['name']]['instanceType'])][jobGroup]['jobsWaitingOnGroup'].remove(currentJob['name'])
                                                    ccqHubVars.jobMappings[currentJob['name']]['status'] = "CreatingCG"
                                                    ccqHubVars.jobMappings[currentJob['name']]['isCreating'] = "false"
                                                    ccqHubVars.jobMappings[currentJob['name']]['instancesToUse'] = []
                                                    values = ccqsubMethods.updateJobInDB({"status": "CreatingCG"}, currentJob['name'])
                                                    ccqHubVars.jobMappings[currentJob['name']]['computeGroup'] = ""
                                                writeCcqVarsToFile()
                                        else:
                                            #If the job is in the CreatingCG state then move forward
                                            print "Still waiting for compute group to create"
                            else:
                                #Just submit the job and move on
                                jobSubmission(currentJob, scheduler)
                    except Exception as e:
                        print "Encountered a breaking error on job " + str(listOfJobsToProcess[job[0]])
                        print traceback.format_exc()
                    print "The time at the end of delegate tasks is: " + str(time.time())


def main():
        #Set the global variables that ccq will use throughout its operation
        ccqHubVars.init()
        print "Successfully initialized the global variables for ccq."

        #Set the locks for the threads to ensure data integrity and to ensure multiple threads do not write to the same ccq variable or ccq status file at once
        ccqHubVars.ccqVarLock = threading.RLock()
        ccqHubVars.ccqFileLock = threading.RLock()
        ccqHubVars.ccqHubDBLock = threading.RLock()

        #ccqCleanupThread = threading.Thread(target=monitorJobs)
        #ccqCleanupThread.start()
        #print "Successfully started the monitorJobsAndInstances thread to check and monitor the jobs."

        ccqDelegateTasksThread = threading.Thread(target=delegateTasks)
        ccqDelegateTasksThread.start()

        #Run forever and check to make sure the threads are running
        while True:
            time.sleep(120)
            if not ccqDelegateTasksThread.is_alive:
                print "UHOH WE DIED AT SOME POINT"
                ccqDelegateTasksThread.start()
            else:
                print "The ccqDelegateTasksThread is running."
            # if not ccqCleanupThread.is_alive:
            #     print "UHOH WE DIED AT SOME POINT"
            #     ccqCleanupThread.start()
            # else:
            #     print "The ccqCleanupThread is running."
main()