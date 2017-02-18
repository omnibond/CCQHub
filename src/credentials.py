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
import policies
import ccqHubMethods


def checkAdminRights():
    import ctypes
    import os

    try:
        # Check admin rights on Unix
        isAdmin = os.getuid()
        if isAdmin == 0:
            return {"status": "success", "payload": True}
        else:
            return {"status": "failure", "payload": False}
    except AttributeError:
        # Check admin rights Windows
        isAdmin = ctypes.windll.shell32.IsUserAnAdmin()
        if isAdmin:
            return {"status": "success", "payload": True}
        else:
            return {"status": "failure", "payload": False}





def evaluatePermssions(subject, actions):
    # Get the valid actions and requiredAttributes
    actionsAndRequiredAttributes = policies.getValidActionsAndRequiredAttributes()

    typeOfSubject = subject['type']
    response = ccqHubMethods.queryObj(None, "RecType-Identity-" + str(typeOfSubject) + "-" + str(subject['subject']) + "-name-", "query", "json", "beginsWith")
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
