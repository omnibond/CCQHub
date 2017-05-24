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

import base64
from random import randint

from bottle import request, route, get, post, error
import time
import sys
import os

# sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
# from Slurm import SlurmScheduler
# from Torque import TorqueScheduler
# from Condor import CondorScheduler
# from Openlava import OpenlavaScheduler

import argparse
import urllib2
import json
import traceback
import ccqHubMethods

#tempJobScriptLocation = ClusterMethods.tempScriptJobLocation
#tempJobOutputLocation = ClusterMethods.tempJobOutputLocation


@route('/ccqHubStat', method='POST')
def ccqHubstat():
    VARS = request.json
    jobId = VARS["jobId"]
    userName = VARS["userName"]
    password = VARS["password"]
    verbose = VARS['verbose']
    instanceId = VARS['instanceId']
    jobNameInScheduler = VARS['jobNameInScheduler']
    requestedSchedulerName = VARS['schedulerName']
    schedulerType = VARS['schedulerType']
    schedulerInstanceId = VARS['schedulerInstanceId']
    schedulerInstanceName = VARS['schedulerInstanceName']
    schedulerInstanceIp = VARS['schedulerInstanceIp']
    printErrors = VARS['printErrors']
    valKey = VARS['valKey']
    dateExpires = VARS['dateExpires']
    certLength = VARS['certLength']
    jobInfoRequest = VARS['jobInfoRequest']
    ccAccessKey = VARS['ccAccessKey']
    targetAddresses = VARS['targetAddresses']
    targetName = VARS['targetName']
    remoteUserName = VARS['remoteUserName']

    userName = remoteUserName

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        encodedUserName = ccqHubMethods.encodeString("ccqunfrval", str(values['payload']['userName']))
        encodedPassword = ccqHubMethods.encodeString("ccqpwfrval", str(values['payload']['password']))
        cert = values['payload']['cert']

    # We will need to get the jobId of the job at the scheduler out of the DB before sending it to the ccq service
    jobIdInCcq = None
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] != "success":
        return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
    else:
        results = results['payload']
        for job in results:
            try:
                jobIdInCcq = job['jobIdInCcq']
            except Exception as e:
                # The job has not yet been submitted to the remote scheduler by ccqHubHandler. We need to delete the job before it is processed.
                ccqHubMethods.updateJobInDB({"status": "Deleting"}, jobId)

    # If they want the verbose status of the job we need to forward the request to the remote scheduler and get the status from it.
    if str(verbose).lower() == "true":
        # TODO need to be able to loop through the multiple addresses for a target. Currently we just use the first one.
        url = "https://" + str(targetAddresses)[0] + "/srv/ccqstat"
        final = {"jobId": str(jobIdInCcq), "userName": str(encodedUserName), "password": str(encodedPassword), "verbose": verbose, "instanceId": None, "jobNameInScheduler": None, "schedulerName": str(targetName), 'schedulerType': None, 'schedulerInstanceId': None, 'schedulerInstanceName': None, 'schedulerInstanceIp': None, 'printErrors': str(printErrors), "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "jobInfoRequest": False, "ccAccessKey": str(ccAccessKey)}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            print "WE MADE IT!!!!!!"
            print res
            #res = json.loads(res)
        except Exception as e:
            print traceback.format_exc(e)

    # If they don't want the verbose status, we just get the status out of the DB, format it and return it.
    else:
        jobInformation = []
        if jobId == "all":
            # Need to list all the jobs for the username that they provide
            results = ccqHubMethods.queryObj(None, "RecType-Job-userName-" + str(userName) + "-", "query", "dict", "beginsWith")
            if results['status'] != "success":
                return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
            else:
                results = results['payload']
                for job in results:
                    jobInformation.append(job)
        else:
            results = ccqHubMethods.queryObj(None, "RecType-Job-name-" + str(jobId) + "-", "query", "dict", "beginsWith")
            if results['status'] != "success":
                return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
            else:
                results = results['payload']
                if len(results) == 0:
                    return {"status": "success", "payload": {"message": "The requested job id does not exist. Please check the job id and try again.", "cert": str(cert)}}
                for job in results:
                    jobInformation.append(job)
        values = ccqHubMethods.formatCcqstatOutput(jobInformation)
        if values['status'] == "success":
            return {"status": "success", "payload": {"message": str(values['payload']), "cert": str(cert)}}


@route('/ccqHubDel', method='POST')
def ccqHubdel():
    VARS = request.json
    jobId = VARS["jobId"]
    userName = VARS["userName"]
    password = VARS["password"]
    jobForceDelete = VARS['jobForceDelete']
    valKey = VARS['valKey']
    dateExpires = VARS['dateExpires']
    certLength = VARS['certLength']
    ccAccessKey = VARS['ccAccessKey']
    remoteUserName = VARS['remoteUserName']

    userName = remoteUserName

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        encodedUserName = ccqHubMethods.encodeString("ccqunfrval", str(values['payload']['userName']))
        cert = values['payload']['cert']

    # We will need to get the jobId of the job at the scheduler out of the DB before sending it to the ccq service
    jobIdInCcq = None
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] != "success":
        return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
    else:
        targetAddresses = []
        results = results['payload']
        if len(results) == 0:
            return {"status": "success", "payload": {"message": "The requested job id does not exist. Please check the job id and try again.", "cert": str(cert)}}
        for job in results:
            try:
                jobIdInCcq = job['jobIdInCcq']
                targetAddresses = job['targetAddresses']
            except Exception as e:
                # The job has not yet been submitted to the remote scheduler by ccqHubHandler. We need to delete the job before it is processed.
                ccqHubMethods.updateJobInDB({"status": "Deleting"}, jobId)

    # TODO need to be able to loop through the multiple addresses for a target. Currently we just use the first one.
    url = "https://" + str(targetAddresses)[0] + "/srv/ccqdel"
    final = {"jobId": str(jobIdInCcq), "userName": str(encodedUserName), "instanceId": None, "jobNameInScheduler": None, "password": "", "jobForceDelete": jobForceDelete, 'schedulerType': None, 'schedulerInstanceId': None, 'schedulerInstanceName': None, 'schedulerInstanceIp': None, "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "ccAccessKey": str(ccAccessKey)}
    data = json.dumps(final)
    headers = {'Content-Type': "application/json"}
    req = urllib2.Request(url, data, headers)
    try:
        res = urllib2.urlopen(req).read().decode('utf-8')
        print "WE MADE IT!!!!!!"
        print res
        #res = json.loads(res)
    except Exception as e:
        print traceback.format_exc(e)


@route('/ccqHubSub', method='POST')
def ccqHubsub():
    VARS = request.json
    jobScriptLocation = VARS['jobScriptLocation']
    jobScriptText = VARS['jobScriptFile']
    ccOptionsParsed = VARS['ccOptionsCommandLine']
    jobName = VARS['jobName']
    jobMD5Hash = VARS["jobMD5Hash"]
    userName = VARS["userName"]
    password = VARS["password"]
    valKey = VARS['valKey']
    dateExpires = VARS['dateExpires']
    certLength = VARS['certLength']
    ccAccessKey = VARS['ccAccessKey']
    targetName = VARS['targetName']
    remoteUserName = VARS['remoteUserName']

    # Since we are not calculating the instance type here we need to set it to None. It will be updated later on when we get the info from the ccq scheduler.
    # If the job stays for a local scheduler then this argument is never needed.
    ccOptionsParsed['instanceType'] = ccOptionsParsed['requestedInstanceType']

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        userName = values['payload']['userName']
        password = values['payload']['password']
        cert = values['payload']['cert']

    #TODO figure out how to handle remote username vs regular username currently I think we just want to set the username to the remote userName
    userName = remoteUserName

    #TODO implement pricing calls to the instances

    # The unique identification of the user who submitted the job is the combination of the identity object and the username that they provide for the job
    # They are required to provide a username for the job when submitting through ccqHubsub.
    obj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash, "userName": str(userName), "targetName": str(targetName), "identity": str(identity)}
    values = ccqHubMethods.saveJobScript(**obj)
    if values['status'] != "success":
        return {"status": "error", "payload": {"message": values['payload'], "cert": str(None)}}
    else:
        obj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsParsed": ccOptionsParsed, "userName": str(userName), "isRemoteSubmit": "True", "identity": str(identity), "targetName": str(targetName)}
        results = ccqHubMethods.saveJob(**obj)
        if results['status'] != "success":
            return {"status": "error", "payload": {"message": values['payload'], "cert": str(None)}}
        else:
            generatedJobId = results['payload']['jobId']
            return {"status": "success", "payload": {"message": "The job has successfully been submitted to ccqHub. The job id is: " + str(generatedJobId) + ".\n You may use this job id to lookup the job's status using the ccqstat utility.", "cert": str(cert)}}


def validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey):
    print "Checking user Credentials!"
    validUser = False
    cert = None
    decodedUserName = ""
    decodedPassword = ""
    identity = ""

    if str(ccAccessKey) != "None":
        values = ccqHubMethods.validateAppKey(ccAccessKey)
        if values['status'] == "success":
            validUser = True
            identity = values['payload']['identity']
            cert = None

    #TODO implement these checks in the future when we are doing user/cert authentication
    # Right now we need to just pass back the username since the key is the only thing being validated, if they have the right key the userName doesn't matter
    decodedUserName = str(userName)
    decodedPassword = str(password)

    # #Need to do checks here to make sure the user is authed
    # if valKey != "unpw" and not validUser:
    #     print "Checking Certificate......."
    #     values = ccqHubMethods.validateCertificate(userName, password, valKey, dateExpires)
    #     if values['status'] == "success":
    #         validUser = True
    #         decodedUserName = values['payload']['decUname']
    #         decodedPassword = values['payload']['decPass']
    #         cert = None
    #     else:
    #         print values['payload']
    #         return {"status": "error", "payload": values['payload']}
    #
    # elif not validUser:
    #     decodedUserName = ccqHubMethods.decodeString("ccqunfrval", str(userName))
    #     decodedPassword = ccqHubMethods.decodeString("ccqpwdfrval", str(password))
    #
    #     values = ccqHubMethods.checkUserNamePassword(decodedUserName, decodedPassword)
    #     if values['status'] != 'success':
    #         print values['payload']
    #         cert = None
    #     else:
    #         print values['payload']
    #         validUser = True
    #         #generate new certificate if logging in using username/password and the length of valid certs is greater than 0
    #         if int(certLength) > 0:
    #             certSuccess = gencclogincert(decodedUserName, decodedPassword, certLength)
    #             if certSuccess['status'] != "success":
    #                 cert = {"error": certSuccess['payload']}
    #             else:
    #                 cert = certSuccess['payload']
    #         else:
    #             cert = None

    if not validUser:
        return {"status": "error", "payload": "Invalid credentials. Please try again."}
    else:
        return {"status": "success", "payload": {"cert": cert, "identity": str(identity), "userName": str(decodedUserName), "password": str(decodedPassword)}}
