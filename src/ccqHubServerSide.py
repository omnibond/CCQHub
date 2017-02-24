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

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
from Slurm import SlurmScheduler
from Torque import TorqueScheduler
from Condor import CondorScheduler
from Openlava import OpenlavaScheduler

import argparse
import urllib2
import json
import traceback
import ccqHubMethods
import commands
import math
from datetime import date, datetime, timedelta
import platform
import salt.config
import salt.utils.event

#tempJobScriptLocation = ClusterMethods.tempScriptJobLocation
#tempJobOutputLocation = ClusterMethods.tempJobOutputLocation


@route('/ccqHubstat', method='POST')
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


@route('/ccqHubdel', method='POST')
def ccqHubdel():
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
    valKey = VARS['valKey']
    dateExpires = VARS['dateExpires']
    certLength = VARS['certLength']
    ccAccessKey = VARS['ccAccessKey']



@route('/ccqHubsub', method='POST')
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

    values = validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey)
    if values['status'] != "success":
        #Credentials failed to validate on the server send back error message and failure
        return {"status": "failure", "payload": {"message": str(values['payload']), "cert": str(None)}}
    else:
        userName = None
        password = None
        cert = values['payload']['cert']

    spotPrice = ccOptionsParsed["spotPrice"]
    useSpot = ccOptionsParsed["useSpot"]
    volumeType = ccOptionsParsed["volumeType"]
    if volumeType == "ssd":
        volumeType = "gp2"
    elif volumeType == "magnetic":
        volumeType = "standard"
    elif volumeType == "ssdiops":
        return {"status": "error", "payload": {"message": "The volume type ssdiops is not currently supported by ccqsub at this time.", "cert": str(cert)}}
    else:
        return {"status": "error", "payload": {"message": "The volume type " + str(volumeType) + " is not a valid AWS EBS Volume type please try again.", "cert": str(cert)}}

    schedulerToUse = ccOptionsParsed["schedulerToUse"]
    print "schedulerToUse: " + str(schedulerToUse)
    schedType = ccOptionsParsed["schedType"]

    #TODO implement pricing calls to the instances
    #justPrice = str(ccOptionsParsed["justPrice"])

    #if str(justPrice).lower() == "true":
    #    parameters = {"ccOptionsParsed": ccOptionsParsed}
    #    output = ccqsubMethods.getInstanceType(parameters)
    #    if output['status'] != "success":
    #        print output['payload']
    #        return {"status": "error", "payload": {"message": output['payload']['error'], "cert": str(cert)}}

    #    calculatedPrice = 0
    #
    #     if spotPrice is not None:
    #         #Need to get spot pricing here!!
    #         results = ccqsubMethods.calculatePriceForJob(output['payload']['instanceType'], ccOptionsParsed["numberOfInstancesRequested"], "37", ccOptionsParsed['volumeType'], "OnDemandPrice", True, ccOptionsParsed['spotPrice'])
    #         if results['status'] != "success":
    #             print str(results['payload'])
    #             return {"status": "error", "payload": {"message": results['payload']['error'], "cert": str(cert)}}
    #         else:
    #             calculatedPrice = round(results['payload'], 3)
    #
    #     else:
    #         #Need to get regular pricing here!!
    #         results = ccqsubMethods.calculatePriceForJob(output['payload']['instanceType'], ccOptionsParsed["numberOfInstancesRequested"], "37", ccOptionsParsed['volumeType'], "OnDemandPrice", False, None)
    #         if results['status'] != "success":
    #             print str(results['payload'])
    #             return {"status": "error", "payload": {"message": results['payload']['error'], "cert": str(cert)}}
    #         else:
    #             calculatedPrice = round(results['payload'], 3)
    #
    #     return {"status": "success", "payload": {"message": "The AWS calculated for this job based upon the requested parameters is: " + str(output['payload']['instanceType']) + " with an estimated cost of $" + str(calculatedPrice), "cert": str(cert)}}

    schedulerIpInfo = ccqsubMethods.getSchedulerIPInformation(schedulerToUse, schedType)
    if schedulerIpInfo['status'] == 'success':
        schedulerIpAddress = schedulerIpInfo['payload']["schedulerIpAddress"]
        schedulerType = schedulerIpInfo['payload']['schedulerType']
        schedulerName = schedulerIpInfo['payload']['schedName']
        schedulerInstanceId = schedulerIpInfo['payload']['schedulerInstanceId']
        schedulerInstanceName = schedulerIpInfo['payload']['instanceName']

        ccOptionsParsed["schedulerToUse"] = str(schedulerName)
        ccOptionsParsed['schedType'] = str(schedulerType)

    else:
        print schedulerIpInfo
        return {"status": "error", "payload": {"message": schedulerIpInfo['payload'], "cert": str(cert)}}
        #print "Unable to obtain the information about the " + str(schedulerToUse) + " Scheduler that was requested! Please try again in a few minutes!"
        #sys.exit(0)

    #Already decoded up at the top so no reason to re-decode it.
    obj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptFile": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash, "userName": str(userName), "password": str(password), "isRemoteSubmit": "True"}
    values = ccqHubMethods.readyJobForScheduler(obj)
    return {"status": str(values['status']), "payload": {"message": values["payload"], "cert": str(cert)}}


def validateCreds(userName, password, dateExpires, valKey, certLength, ccAccessKey):
    print "Checking user Credentials!"
    validUser = False

    if str(ccAccessKey) != "None":
        values = ccqHubMethods.validateAppKey(ccAccessKey)
        if values['status'] == "success":
            validUser = True
            #decodedUserName = values['payload']['decUname']
            #decodedPassword = values['payload']['decPass']
            cert = None

    #TODO implement these checks in the future when we are doing user/cert authentication
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
        return {"status": "error", "payload": "Invalid username or password. Please try again."}
    else:
        return {"status": "success", "payload": {"cert": cert}}