# Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of OpenCCQ.

# OpenCCQ is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# OpenCCQ is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with OpenCCQ.  If not, see <http://www.gnu.org/licenses/>.
import traceback
import json
import urllib2
import time
import paramiko
import os

import src.ccqHubMethods as ccqHubMethods
import src.ccqHubVars as ccqHubVars
import src.credentials as credentials

tempJobScriptLocation = ccqHubVars.tempJobScriptLocation
tempJobOutputLocation = ccqHubVars.tempJobOutputLocation
logDirectory = ccqHubVars.logDirectory


class Monitoring:
    def __init__(self):
        ccqHubVars.init()
        pass

    def monitorCloudJobs(self, logger):
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

    def monitorLocalJobs(self, logger):
        #TODO put stuff to monitor local jobs here.
        logger.info("Put stuff to monitor local jobs here.")

    def checkToSeeIfJobStillRunningOnCluster(self, job, scheduler, logger):
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
                ccqHubMethods.writeCcqVarsToFile()

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

    def copyJobOutputFilesToSpecifiedLocation(self, job, logger):
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
