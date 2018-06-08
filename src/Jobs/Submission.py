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

import time
import json
import urllib2
import traceback

import src.ccqHubMethods as ccqHubMethods
import src.ccqHubVars as ccqHubVars
from src.Schedulers.Torque import TorqueScheduler
from src.Schedulers.Slurm import SlurmScheduler


class Submission:
    def __init__(self):
        ccqHubVars.init()
        pass

    def createSchedulerObject(self, schedName, schedType, schedulerInstanceId, schedulerHostName, schedulerIpAddress, clusterName):
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
        except Exception as e:
            return {"status": "error", "payload": "Unable to create the scheduler object"}

    def localJobSubmission(self, jobObj, logger):
        values = self.createSchedulerObject(jobObj['targetName'], jobObj['schedType'], None, None, None, None)
        logger.info("Need to do some work here, shouldn't be too bad actually. Just need to modify the Scheduler file to fit the needs of the local scheduler")

    def cloudJobSubmission(self, jobObj, logger):
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
                                    logger.info(str(res['payload']['message']) + "\n\n")
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
