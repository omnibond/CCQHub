# CCQHub
CloudyCluster Queue (CCQ) is a meta-scheduler to link disparate HPC environments with the Job as the payload of work, CCQHub is a job router of sorts for HPC and Parallel jobs.

The goals of this project are to provide a meta-scheduler that can accomplish the following:

- Provide a simple familiar way for parallel computational users and scientists to specify where jobs can process including: local HPC resources, National HPC resources, Cloudy HPC resources

- Use the Job as the basic driver or element of work that can drive dynamic provisioning of cloud resources such as cloud instances for parallel computation, provision data sets to sites, or send jobs to data lakes for processing.

- support Hueristics for automated job handling such as wait time, data location, codes used for job, PI, funding source, and others...

This project is seeded with many of the components of CCQ that has been a part of CloudyCluster since its release, we hope this new open source life will encourage collaboration in concepts and coding.   Fundamental support for job handling, front ending Torque, Slurm and Open Lava are all provided with this initial code.   Basic cost estimating and transmission for autoscaling resources in the cloud are also part of the initial code base.

THe initial work will be to develop an on premise database and surrounding security infrastructure for cross environment HPC submission will be some of the initial work, along with recombining the pieces as a separate project.
