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

import sys
import time
import traceback
import threading
import logging

from Jobs.Monitoring import Monitoring
from Jobs.Submission import Submission
from Jobs.Deletion import Deletion
import ccqHubMethods
import ccqHubVars

# Create the classes that contain the code for job Monitoring, Submission, and Deletion
monitoringClass = Monitoring()
submissionClass = Submission()
deletionClass = Deletion()

tempJobScriptLocation = ccqHubVars.tempJobScriptLocation
tempJobOutputLocation = ccqHubVars.tempJobOutputLocation
logDirectory = ccqHubVars.logDirectory


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


def monitorJobs(logger, schedulerType):
    #Run for the duration of the ccqLauncher and monitor the jobs and instances for things that need deleted
    while True:
        try:
            time.sleep(30)
            if schedulerType == "cloud":
                logger.info("The timestamp when start monitorCloudJobs is: " + str(time.time()))
                monitoringClass.monitorCloudJobs(logger)
                logger.info("The timestamp when end monitorCloudJobs is: " + str(time.time()))
            elif schedulerType == "local":
                logger.info("The timestamp when start monitorLocalJobs is: " + str(time.time()))
                monitoringClass.monitorLocalJobs(logger)
                logger.info("The timestamp when end monitorLocalJobs is: " + str(time.time()))
        except Exception as e:
            logger.info("There was an error encountered in monitorJobs:\n")
            logger.info(traceback.format_exc(e))


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
                        except Exception as e:
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
                                deletionClass.cleanupDeletedJob(currentJob['name'], logger)
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
                                    values = submissionClass.cloudJobSubmission(currentJob, logger)
                                    logger.info(values)

                                elif values['payload']['nextStep'] == "localSubmit":
                                    # TODO do stuff here for the local job submission implementation
                                    values = submissionClass.localJobSubmission(currentJob, logger)
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
        monitorCloudJobsThreadLogger = createLoggerForThread("monitorCloudJobsThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_monitor_cloud_jobs.log", False, False)
        monitorJobsLocalThreadLogger = createLoggerForThread("monitorLocalJobsThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_monitor_local_jobs.log", False, False)

        # Create the delegateTasks logger
        delegateTasksThreadLogger = createLoggerForThread("delegateTasksThreadLogger", "DEBUG", None, str(prefix) + "ccqHub_delegate_tasks.log", False, False)

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
                print("The ccqDelegateTasksThread encountered an issue and has died.")
                ccqDelegateTasksThread.start()
            else:
                print("The ccqDelegateTasksThread is running.")

            if not cloudJobMonitorThread.is_alive:
                print("The cloudJobMonitorThread encountered an issue and has died.")
                cloudJobMonitorThread.start()
            else:
                print("The cloudJobMonitorThread is running.")

            if not localJobMonitorThread.is_alive:
                print("The localJobMonitorThread encountered an issue and has died.")
                localJobMonitorThread.start()
            else:
                print("The localJobMonitorThread is running.")


main()

