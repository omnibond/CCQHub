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
import ccqHubVars
import ccqHubMethods
import credentials

#tempJobScriptLocation = ClusterMethods.tempScriptJobLocation
#tempJobOutputLocation = ClusterMethods.tempJobOutputLocation


@route('/ccqHubStat', method='POST')
def ccqHubStat():
    VARS = request.json
    jobId = VARS["jobId"]
    userName = VARS["userName"]
    password = VARS["password"]
    verbose = VARS['verbose']
    printErrors = VARS['printErrors']
    valKey = VARS['valKey']
    dateExpires = VARS['dateExpires']
    certLength = VARS['certLength']
    jobInfoRequest = VARS['jobInfoRequest']
    ccAccessKey = VARS['ccAccessKey']
    targetName = VARS['targetName']
    remoteUserName = VARS['remoteUserName']
    listAllJobs = VARS['listAllJobs']
    additionalActionsAndPermissionsRequired = VARS['additionalActionsAndPermissionsRequired']

    targetAddresses = []
    targetProxyKeys = []

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        cert = values['payload']['cert']
        userName = str(values['payload']['userName'])
        password = str(values['payload']['password'])
        encodedUserName = ccqHubMethods.encodeString("ccqunfrval", str(values['payload']['userName']))
        encodedPassword = ccqHubMethods.encodeString("ccqpwfrval", str(values['payload']['password']))

    # If they want the verbose status of the job we need to forward the request to the remote scheduler and get the status from it.
    if str(verbose).lower() == "true":
        jobIdInCcq = None
        if jobId != "all":
            # We will need to get the jobId of the job at the scheduler out of the DB before sending it to the ccq service
            results = ccqHubMethods.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
            if results['status'] != "success":
                return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
            else:
                results = results['payload']
                for job in results:

                    # Check to see if the job has been submitted to the remote scheduler before continuing.
                    # TODO probably need to determine just what statuses we want to exclude here and how we are going to handle that
                    if job['status'] != "ccqHubSubmitted" and job['status'] != "Running":
                        return {"status": "success", "payload": {"message": "The verbose status is available only for jobs that are in the ccqHubSubmitted or Running states.", "cert": str(None)}}

                    try:
                        # Need to see if the job has been submitted to ccq in the cloud. If it has then we move on if it hasn't we return the non-verbose status and move on.
                        jobIdInCcq = job['jobIdInCcq']
                    except Exception as e:
                        # The job has not yet been submitted to the remote scheduler by ccqHubHandler. We need to send back the status that we have in the DB.
                        values = ccqHubMethods.formatCcqstatOutput([job])
                        if values['status'] == "success":
                            return {"status": "success", "payload": {"message": str(values['payload']), "cert": str(cert)}}

                    targetAddresses = json.loads(job["targetAddresses"])
                    targetProxyKeys = json.loads(job["targetProxyKeys"])
                    targetProtocol = job["targetProtocol"]
                    targetAuthType = job["targetAuthType"]

        else:
            # The user wants to see the verbose status of all jobs on the remote target
            response = ccqHubMethods.queryObj(None, "RecType-Target-targetName-" + str(targetName) + "-schedulerType-", "query", "json", "beginsWith")
            if response['status'] == "success":
                results = response['payload']
                for target in results:
                    targetAddresses = json.loads(target["targetAddress"])
                    targetProxyKeys = json.loads(target["proxyKey"])
                    targetProtocol = target["protocol"]
                    targetAuthType = target["authType"]
            jobIdInCcq = "all"

        # Need to try all of the targetAddresses. Once one has successfully returned we quit. If we get an error in the connection we retry with the other addresses
        submittedSuccessfully = False
        # Need to try all of the target proxy keys. Once one has successfully returned we exit, if there is an error due to the key we retry.
        for address in targetAddresses:
            badURL = False
            if not submittedSuccessfully:
                try:
                    for proxyKey in targetProxyKeys:
                        if not badURL:
                            url = "https://" + str(address) + "/srv/ccqstat"
                            final = {"jobId": str(jobIdInCcq), "userName": str(encodedUserName), "password": str(encodedPassword), "verbose": verbose, "instanceId": None, "jobNameInScheduler": None, "schedulerName": str(targetName), 'schedulerType': None, 'schedulerInstanceId': None, 'schedulerInstanceName': None, 'schedulerInstanceIp': None, 'printErrors': str(printErrors), "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "jobInfoRequest": False, "ccAccessKey": str(proxyKey), "printOutputLocation": False, "printInstancesForJob": False, "remoteUserName": str(remoteUserName), "databaseInfo": False}
                            data = json.dumps(final)
                            headers = {'Content-Type': "application/json"}
                            try:
                                req = urllib2.Request(url, data, headers)
                                res = urllib2.urlopen(req).read().decode('utf-8')
                                res = json.loads(res)
                            except Exception as e:
                                # We couldn't connect to the url so we need to try the other one.
                                badURL = True
                                print(''.join(traceback.format_exc(e)))
                            if not badURL:
                                if res['status'] == "failure":
                                    if proxyKey is not None:
                                        print("The key is not valid, please check your key and try again.")
                                elif res['status'] == "error":
                                    #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
                                    print(res['payload']['message'] + "\n\n")
                                    return {"status": "error", "payload": {"message": res['payload']['message'], "cert": str(cert)}}
                                elif res['status'] == "success":
                                    submittedSuccessfully = True
                                    if "There are currently no jobs in the Submitted or Running state" in str(res['payload']['message']):
                                        return {"status": "success", "payload": {"message": "There are currently no jobs running at the " + str(targetName) + " Target at this time.", "cert": str(cert)}}
                                    elif "The -v argument is current only available for jobs that are in the" in str(res['payload']['message']):
                                        if jobId != "all":
                                            return {"status": "success", "payload": {"message": "The job is still being processed by the remote ccq scheduler so there is no verbose information to display at this time.\nThe current non-verbose status of the job is: " + str(job['status']), "cert": str(cert)}}
                                    else:
                                        return {"status": "success", "payload": {"message": res['payload']['message'], "cert": str(cert)}}
                except Exception as e:
                    # We encountered an unexpected exception
                    print(''.join(traceback.format_exc(e)))
        return {"status": "error", "payload": {"message": "Unable to successfully get the status of the job(s) specified. Please re-check the credentials and try again.", "cert": str(cert)}}

    # If they don't want the verbose status, we just get the status out of the DB, format it and return it.
    else:
        jobInformation = []
        if jobId == "all":
            if str(listAllJobs).lower() != "true":
                # Need to list all the jobs for the username that they provide
                results = ccqHubMethods.queryObj(None, "RecType-Job-userName-" + str(userName) + "-", "query", "dict", "beginsWith")
            else:
                # Need to list all the jobs for all the users
                results = ccqHubMethods.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
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
def ccqHubDel():
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
    additionalActionsAndPermissionsRequired = VARS['additionalActionsAndPermissionsRequired']

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        userName = values['payload']['userName']
        password = values['payload']['password']
        cert = values['payload']['cert']
        encodedUserName = ccqHubMethods.encodeString("ccqunfrval", str(values['payload']['userName']))

    # We will need to get the jobId of the job at the scheduler out of the DB before sending it to the ccq service
    jobIdInCcq = None
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] != "success":
        return {"status": "error", "payload": {"message": str(results['payload']), "cert": str(None)}}
    else:
        results = results['payload']
        if len(results) == 0:
            return {"status": "success", "payload": {"message": "The requested job id does not exist. Please check the job id and try again.", "cert": str(cert)}}
        for job in results:
            print("JOB IS: " + str(job))
            targetAddresses = json.loads(job["targetAddresses"])
            targetProxyKeys = json.loads(job["targetProxyKeys"])
            targetProtocol = job["targetProtocol"]
            targetAuthType = job["targetAuthType"]
            try:
                # Need to see if the job has been submitted to ccq in the cloud. If it has then we move on if it hasn't then we mark the job to be deleted by the ccqHandler process and move on.
                jobIdInCcq = job['jobIdInCcq']
                # Need to try and submit to all of the targetAddresses. Once one has successfully returned we quit. If we get an error in the connection we retry with the other addresses
                deletedSuccessfully = False
                # Need to try and use all of the target proxy keys. Once one has returned successfully we exit, if there is an error due to the key we retry.
                for address in targetAddresses:
                    badURL = False
                    if not deletedSuccessfully:
                        try:
                            for proxyKey in targetProxyKeys:
                                if not badURL:
                                    url = "https://" + str(address) + "/srv/ccqdel"
                                    final = {"jobId": str(jobIdInCcq), "userName": str(encodedUserName), "instanceId": None, "jobNameInScheduler": None, "password": "", "jobForceDelete": jobForceDelete, 'schedulerType': None, 'schedulerInstanceId': None, 'schedulerInstanceName': None, 'schedulerInstanceIp': None, "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "ccAccessKey": str(proxyKey), "remoteUserName": str(remoteUserName)}
                                    data = json.dumps(final)
                                    headers = {'Content-Type': "application/json"}
                                    try:
                                        req = urllib2.Request(url, data, headers)
                                        res = urllib2.urlopen(req).read().decode('utf-8')
                                        res = json.loads(res)
                                    except Exception as e:
                                        # We couldn't connect to the url so we need to try the other one.
                                        badURL = True
                                        print(''.join(traceback.format_exc(e)))
                                    if not badURL:
                                        if res['status'] == "failure":
                                            if proxyKey is not None:
                                                print("The key is not valid, please check your key and try again.")
                                        elif res['status'] == "error":
                                            #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
                                            print(res['payload']['message'] + "\n\n")
                                            return {"status": "error", "payload": {"message": res['payload']['message'], "cert": str(cert)}}
                                        elif res['status'] == "success":
                                            deletedSuccessfully = True
                                            results = ccqHubMethods.handleObj("delete", job)
                                            if results['status'] != "success":
                                                return {"status": "success", "payload": {"message": results['payload']['message'], "cert": str(cert)}}
                                            return {"status": "success", "payload": {"message": res['payload']['message'], "cert": str(cert)}}
                        except Exception as e:
                            # We encountered an unexpected exception
                            print(''.join(traceback.format_exc(e)))
                return {"status": "error", "payload": {"message": "Unable to successfully delete the job specified. Please re-check the credentials and try again.", "cert": str(cert)}}
            except Exception as e:
                # The job has not yet been submitted to the remote scheduler by ccqHubHandler. We need to delete the job before it is processed.
                ccqHubMethods.updateJobInDB({"status": "Deleting"}, jobId)
                return {"status": "error", "payload": {"message": "The job has been successfully marked to be deleted.", "cert": str(cert)}}
        return {"status": "error", "payload": {"message": "Unable to find the job in the database. Please check the job Id and try again.", "cert": str(cert)}}


@route('/ccqHubSub', method='POST')
def ccqHubSub():
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
    additionalActionsAndPermissionsRequired = VARS['additionalActionsAndPermissionsRequired']

    # Since we are not calculating the instance type here we need to set it to None. It will be updated later on when we get the info from the ccq scheduler.
    # If the job stays for a local scheduler then this argument is never needed.
    ccOptionsParsed['instanceType'] = ccOptionsParsed['requestedInstanceType']

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        identity = values['payload']['identity']
        userName = values['payload']['userName']
        password = values['payload']['password']
        cert = values['payload']['cert']

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


def validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired):
    print("Checking user Credentials.")
    validUser = False
    cert = None
    identity = ""

    decodedUserName = ccqHubMethods.decodeString("ccqunfrval", str(userName))
    decodedPassword = ccqHubMethods.decodeString("ccqpwdfrval", str(password))

    if str(ccAccessKey) != "None":
        values = credentials.validateAppKey(ccAccessKey, remoteUserName, additionalActionsAndPermissionsRequired)
        if values['status'] == "success":
            validUser = True
            identity = values['payload']['identity']
            if str(remoteUserName) != "None":
                decodedUserName = remoteUserName
            #decodedPassword = decodedPassword
            cert = None

    elif userName is not None and password is not None:
        pamService = ""
        # Load the PAM service to check from the Config File
        values = ccqHubVars.retrieveSpecificConfigFileKey("PAM Configuration", "service")
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}
        else:
            pamService = values['payload']
        import pam
        p = pam.pam()
        onSystem = p.authenticate(str(decodedUserName), str(decodedPassword), str(pamService))
        if not onSystem:
            return {"status": "error", "payload": "Invalid credentials. Please try again."}

        #TODO Add in other rule logic that would allow us to limit the users on the system that can utilize ccqHub, put in stub method for now
        # Check authorization rules to make sure the user is allowed
        results = checkRestrictions()
        if values['status'] != "success":
            return {"status": "error", "payload": results['payload']}
        else:
            # The user is authenticated and we now need to see if they already have an identity object and if not we need to create them one and generate an app key for them.
            res = ccqHubMethods.queryObj(None, "RecType-Identity-userName-" + str(decodedUserName) + "-name-", "query", "json", "beginsWith")
            if res['status'] != "success":
                return {"status": "error", "payload": res['payload']}
            else:
                foundIdentity = False
                for id in res['payload']:
                    foundIdentity = True
                    identity = id['name']
                if not foundIdentity:
                    # We did not find an identity for the user so we need to create one for them.
                    actions = ccqHubMethods.ccqHubGeneratedIdentityDefaultPermissions
                    userNames = [str(decodedUserName)]
                    results = ccqHubMethods.createIdentity(actions, userNames, False)
                    if results['status'] != "success":
                        return {"status": "error", "payload": results['payload']}
                    else:
                        # We successfully created a new Identity object for the user logging in via PAM.
                        identity = results['payload']['identityUuid']

                        # Evaluate the additionalActions requested if there are any
                        for additionalAction in additionalActionsAndPermissionsRequired:
                            subject = {"subjectType": "identityUuid", "subject": str(identity), "subjectRecType": "Identity"}
                            results = credentials.evaluatePermissions(subject, additionalActionsAndPermissionsRequired[additionalAction])
                            if results['status'] != "success":
                                return {"status": "failure", "payload": results['payload']}

                validUser = True

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


def checkRestrictions():
    # Stub method for when we want to add other restrictions on which local users can utilize ccqHub
    return {"status": "success", "payload": "User is authorized"}
