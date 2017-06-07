# CCQHub
CloudyCluster Queue (CCQ) is a meta-scheduler to link disparate HPC environments with the Job as the payload of work, CCQHub is a job router of sorts for HPC and Parallel jobs.

The goals of this project are to provide a meta-scheduler that can accomplish the following:

- Provide a simple familiar way for parallel computational users and scientists to specify where jobs can process including: local HPC resources, National HPC resources, Cloudy HPC resources

- Use the Job as the basic driver or element of work that can drive dynamic provisioning of cloud resources such as cloud instances for parallel computation, provision data sets to sites, or send jobs to data lakes for processing.

- support Hueristics for automated job handling such as wait time, data location, codes used for job, PI, funding source, and others...

This project is seeded with many of the components of CCQ that has been a part of CloudyCluster since its release, we hope this new open source life will encourage collaboration in concepts and coding.   Fundamental support for job handling, front ending Torque, Slurm and Open Lava are all provided with this initial code.   Basic cost estimating and transmission for autoscaling resources in the cloud are also part of the initial code base.

THe initial work will be to develop an on premise database and surrounding security infrastructure for cross environment HPC submission will be some of the initial work, along with recombining the pieces as a separate project.

# prerequisite
- python 2.7.7
- pip install -U bottle
- pip install -U tornado
- pip install -U paramiko
- pip install -U python-pam
- pip install -U cryptography

# Installation
 (for running as a central meta-scheduler for multiple users, other modes are not implimented yet)
- Download and expand the tar.gz or clone the repo
- cd into CCQHub directory
- run ccqHubInstaller
  - Default options: port 8080 and localhost, auth uses login PAM service, sqlite3 db (only option for now), addToPath only works on linux, defaults to cloud (since cloud was developed first)
- software is installed and tornado is running on port 8080 (or whatever you selected).  If there are no errors in /opt/ccqhub/logs/ccqHubWebServerLogFile.txt (which is catted at the end of the installer)

- Next add a target to ccqhub using ccqHubAddTarget (it prompts for config options as it configures)
   - run ccqHubAddTarget (from opt/ccqHub/bin/) and follow the prompts outlined below:
   - Enter the name of the target to be added (a target is a scheduler or ccq)
   - Enter a scheduler type (torque, slurm, ccq), for ccq enter the address of the login instance.
   - Select protocol (currently supports https for ccq and local - ssh will come in the future).
   - Authorization type appkey (for ccq and you have created a proxy key) or username (for local torque or slurm).
   - Default hub target? (this is the default for that scheduler type through ccq or local)
   - to check the target configuration you can issue the command ccqHubListDefaultTargets or ccqHubListDefaultTargets
   - to modify the targetsyou can use ccqHubModifyTarget (does not prompt, must fill in the options).
 
 - when you ran the installer there are two default identities that have a key associated with each.
   - admin identity and key (the commands look for this key in /opt/ccqHub/.keys/ and are only readable by root)
   - proxy identity and key
   - you can list these by running ccqHubListIdentities -v 
   - when a user runs ccqsub (mentioned later) in PAM mode an identity is automatically created for the user in ccqHub and associated by username to the PAM identity.   To use these identities in ccq in cloudycluster you will need to create the users there using the ccUserAdd (so file permissions and UIDs match).
   - to add an additional identity to ccqHub (for special permissions) run ccqHubAddIdentity
 
# Operation
Now you can submit/monitor jobs through ccqsub, ccqdel, ccqstat commands
  
