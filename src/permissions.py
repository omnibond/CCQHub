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

import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Database"))
from sqlLite3Database import sqlLite3Database

dbInterface = sqlLite3Database()

def getValidActionsAndRequiredAttributes():
    # These identify the valid actions and what attributes the Identity is required to have in order to be granted permission to perform the action
    actionsAndRequiredAttributes = {"ccqHubAdmin": {"ccqHubAdmin": "True"},
                                    "proxyJob": {"proxyJob": "True"},
                                    "submitJob": {"submitJob": "True"}
                                   }
    return actionsAndRequiredAttributes


def evaluatePermssions(subject, actions):
    # Get the valid actions and requiredAttributes
    actionsAndRequiredAttributes = getValidActionsAndRequiredAttributes()

    typeOfSubject = subject['type']
    response = dbInterface.queryObj(None, "RecType-Identity-" + str(typeOfSubject) + "-" + str(subject['subject']), "query", "json", "beginsWith")
    if response['status'] == "success":
        results = response['payload']
        validated = False
        for identity in results:
            # We have the identity object
            for action in actions:
                if action in actionsAndRequiredAttributes:
                    for attribute in actionsAndRequiredAttributes[action]:
                        if attribute in identity:
                            if str(identity[attribute]) == str(actionsAndRequiredAttributes[action][attribute]):
                                validated = True
                            else:
                                validated = False
        if not validated:
            return {"status": "failure", "payload": "This identity is not authorized to perform the requested action(s)."}
        else:
            return {"status": "success", "payload": "This identity is authorized to perform the requested action(s)."}
    else:
        return {"status": "error", "payload": response['payload']}
