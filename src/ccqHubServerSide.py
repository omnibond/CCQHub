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
    schedulerName = VARS['schedulerName']
    schedulerType = VARS['schedulerType']
    schedulerInstanceId = VARS['schedulerInstanceId']
    schedulerInstanceName = VARS['schedulerInstanceName']
    schedulerInstanceIp = VARS['schedulerInstanceIp']

    instanceRecType = ""

    #Stuff for if authed from cert file
    isCert = VARS['isCert']


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

    #Stuff for if authed from cert file
    isCert = VARS['isCert']



@route('/ccqHubsub', method='POST')
def ccqHubsub():
    print "Made it to ccqsub!"

    VARS = request.json
    jobScriptLocation = VARS['jobScriptLocation']
    jobScriptText = VARS['jobScriptText']
    ccOptionsParsed = VARS['ccOptionsCommandLine']
    jobId = VARS["jobId"]
    jobName = VARS['jobName']
    jobMD5Hash = VARS["jobMD5"]
    userName = VARS["userName"]
    password = VARS["password"]
    schedName = VARS['schedName']

    #Stuff for if authed from cert file
    isCert = VARS['isCert']