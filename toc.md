# Table of Contents

- [Introduction](#Introduction)
- [Provisioning ADB: Guidelines for Networking and Security](#Provisioning-ADB-Guidelines-for-Networking-and-Security)
  * [Azure Databricks 101](#Azure-Databricks-101)
  * [Map Workspaces to Business Units](#Map-Workspaces-To-Business-Units)
  * [Deploy Workspaces in Multiple Subscriptions](#Deploy-Workspaces-in-Multiple-Subscriptions)
    + [ADB Workspace Limits](#ADB-Workspace-Limits)
    + [Azure Subscription Limits](#azure-subscription-limits)
  * [Consider Isolating Each Workspace in its own VNet](#Consider-Isolating-Each-Workspace-in-its-own-VNet)
  * [Select the largest CIDR possible for a VNet](#Select-the-largest-CIDR-possible-for-a-VNet)
  * [Do not store any production data in default DBFS folders](#Do-not-store-any-production-data-in-default-DBFS-folders)
  * [Always hide secrets in Key Vault and do not expose them openly in Notebooks](#always-hide-secrets-in-a-key-vault-and-do-not-expose-them-openly-in-notebooks)
- [Developing applications on ADB: Guidelines for selecting clusters](#Developing-applications-on-ADB-Guidelines-for-selecting-clusters)
  * [Support Interactive analytics using shared High Concurrency clusters](#support-interactive-analytics-using-shared-high-concurrency-clusters)
   * [Support Batch ETL workloads with single user ephemeral Standard clusters](#support-batch-etl-workloads-with-single-user-ephemeral-standard-clusters)
   * [Favor Cluster Scoped Init scripts over Global and Named scripts](#favor-cluster-scoped-init-scripts-over-global-and-named-scripts)
   * [Send logs to blob store instead of default DBFS using Cluster Log delivery](#send-logs-to-blob-store-instead-of-default-DBFS-using-Cluster-Log-delivery)
   * [Choose cluster VMs to match workload class](#Choose-cluster-VMs-to-match-workload-class)
   * [Arrive at correct cluster size by iterative performance testing](#Arrive-at-correct-cluster-size-by-iterative-performance-testing)
   * [Tune shuffle for optimal performance](#Tune-shuffle-for-optimal-performance)
   * [Store Data In Parquet Partitions](#Store-Data-In-Parquet-Partitions)
    + [Sub-sub-heading](#sub-sub-heading-2)
- [Monitoring](#Monitoring)
  * [Collect resource utilization metrics across Azure Databricks cluster in a Log Analytics workspace](#Collect-resource-utilization-metrics-across-Azure-Databricks-cluster-in-a-Log-Analytics-workspace)
        + [Querying VM metrics in Log Analytics once you have started the collection using the above document](#Querying-VM-metrics-in-log-analytics-once-you-have-started-the-collection-using-the-above-document)
        
- [Appendix A](#Appendix-A)
  * [Installation for being able to capture VM metrics in Log Analytics](#Installation-for-being-able-to-capture-VM-metrics-in-Log-Analytics)


   
    
    
    
    
# Table of Figures

- [Figure 1: Databricks user menu](#heading)
- [Figure 2: Business Unit Subscription Design Pattern](#heading)
- [Figure 3: Azure Databricks Isolation Domains Workspace](#heading)
- [Figure 4: Hub and Spoke Model](#heading)
- [Figure 5: Interactive clusters](#heading)
- [Figure 6: Ephemeral Job Cluster](#heading)
- [Figure 7: Shuffle vs. no-shuffleu](#heading)

# Table of Tables
- [Table 1: CIDR Ranges](#heading)
- [Table 2: Cluster modes and their characteristics](#heading)
- [Table 3: Batch vs Interactive Workloads](#heading)


# Table of x - save for later
- [Heading](#heading)
  * [Sub-heading](#sub-heading)
    + [Sub-sub-heading](#sub-sub-heading)


# Heading levels

> This is a fixture to test heading levels

<!-- toc -->

## Introduction

Planning, deploying, and running Azure Databricks (ADB) at scale requires one to make many architectural decisions.

While each ADB deployment is unique to an organization's needs we have found that some patterns are common across most successful ADB projects. Unsurprisingly, these patterns are also in-line with modern Cloud-centric development best practices.

This short guide summarizes these patterns into prescriptive and actionable best practices for Azure Databricks. We follow a logical path of planning the infrastructure, provisioning the workspaces,developing Azure Databricks applications, and finally, running Azure Databricks in production.

The audience of this guide are system architects, field engineers, and development teams of customers, Microsoft, and Databricks. Since the Azure Databricks product goes through fast iteration cycles, we have avoided recommendations based on roadmap or Private Preview features.

Our recommendations should apply to a typical Fortune 500 enterprise with at least intermediate level of Azure and Databricks knowledge. We've also classified each recommendation according to its likely impact on solution's quality attributes. Using the **Impact** factor, you can weigh the recommendation against other competing choices. Example: if the impact is classified as “Very High”, the implications of not adopting the best practice can have a significant impact on your deployment.

As ardent cloud proponents, we value agility and bringing value quickly to our customers. Hence, we’re releasing the first version somewhat quickly, omitting some important but advanced topics in the interest of time. We will cover the missing topics and add more details in the next round, while sincerely
hoping that this version is still useful to you.

## Provisioning ADB: Guidelines for Networking and Security

Azure Databricks (ADB) deployments for very small organizations, PoC applications, or for personal education hardly require any planning. You can spin up a Workspace using Azure Portal in a matter of minutes, create a Notebook, and start writing code.

Enterprise-grade large scale deployments are a different story altogether. Some upfront planning is necessary to avoid cost overruns, throttling issues, etc. In particular, you need to understand:

  * Networking requirements of Databricks
  * The number and the type of Azure networking resources required to launch clusters
  * Relationship between Azure and Databricks jargon: Subscription, VNet., Workspaces, Clusters, Subnets, etc.
  * Overall Capacity Planning process: where to begin, what to consider?


Let’s start with a short Azure Databricks 101 and then discuss some best practices for scalable and secure deployments.


## Azure Databricks 101

ADB is a Big Data analytics service. Being a Cloud Optimized managed PaaS offering, it is designed to hide the underlying distributed systems and networking complexity as much as possible from the end user. It is backed by a team of support staff who monitor its health, debug tickets filed via Azure, etc. This allows ADB users to focus on developing value generating apps rather than stressing over infrastructure management.

You can deploy ADB using Azure Portal or using ARM templates. One successful ADB deployment produces exactly one Workspace, a space where users can log in and author analytics apps. It comprises the file browser, notebooks, tables, clusters, DBFS storage, etc. More importantly, Workspace is a fundamental isolation unit in Databricks. All workspaces are expected to be completely isolated from each other -- i.e., we intend that no action in one workspace should noticeably impact another workspace.

Each workspace is identified by a globally unique 53-bit number, called ***Workspace ID or Organization ID***. The URL that a customer sees after logging in always uniquely identifies the workspace they are using:

*https://regionName.azuredatabricks.net/?o=workspaceId*

Azure Databricks uses Azure Active Directory (AAD) as the exclusive Identity Provider and there’s a seamless out of the box integration between them. Any AAD member belonging to the Owner or Contributor role can deploy Databricks and is automatically added to the ADB members list upon first login. If a user is not a member of the Active Directory tenant, they can’t login to the workspace.

Azure Databricks comes with its own user management interface. You can create users and groups in a workspace, assign them certain privileges, etc. While users in AAD are equivalent to Databricks users, by default AAD roles have no relationship with groups created inside ADB. ADB also has a special group called Admin, not to be confused with AAD’s admin.

The first user to login and initialize the workspace is the workspace ***owner***. This person can invite other users to the workspace, create groups, etc. The ADB logged in user’s identity is provided by AAD, and shows up under the user menu in Workspace:

![Figure 1: Databricks user menu](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure1.PNG "Figure 1: Databricks user menu")

*Figure 1: Databricks user menu*

  <!--
                     *Figure 1: Databricks user menu*
<p align="center">
    <img width="460" height="300" src="http://www.fillmurray.com/460/300">
</p>
-->
  


## Map Workspaces to Business Units
*Impact: Very High*

Though partitioning of workspaces depends on the organization structure and scenarios, it is generally recommended to partition workspaces based on a related group of people working together collaboratively. This also helps in streamlining your access control matrix within your workspace (folders, notebooks etc.) and also across all your resources that the workspace interacts with (storage, related data stores like Azure SQL DB, Azure SQL DW etc.). This type of division scheme is also known as the Business Unit Subscription design pattern and aligns well with Databricks chargeback model.


![Figure 2: Business Unit Subscription Design Pattern](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure2.PNG "Figure 2: Business Unit Subscription Design Pattern")

*Figure 2: Business Unit Subscription Design Pattern*


## Deploy Workspaces in Multiple Subscriptions
*Impact: Very High*

Customers commonly partition workspaces based on teams or departments and arrive at that division naturally. But it is also important to partition keeping Azure Subscription and ADB Workspace level limits in mind.


### ADB Workspace Limits
Azure Databricks is a multitenant service and to provide fair resource sharing to all regional customers, it imposes limits on API calls. These limits are expressed at the Workspace level and are due to internal ADB components. For instance, you can only run up to 150 concurrent jobs in a workspace. Beyond that, ADB will deny your job submissions. There are also other limits such as max hourly job submissions, etc.
  
  
Key workspace limits are:

  * There is a limit of **1000** scheduled jobs that can be seen in the UI
  * The maximum number of jobs that a workspace can create in an hour is **1000**
  * At any time, you cannot have more than **150 jobs** simultaneously running in a workspace
  * There can be a maximum of **150 notebooks or execution contexts** attached to a cluster    


### Azure Subscription Limits
Next, there are {FLAG THIS  TO PREMAL IS THIS SUPPOSED TO BE A LINK?} Azure limits [Azure limits](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/toc.md) to consider since ADB deployments are built on top of the Azure infrastructure.

Key Azure limits are:
  * Storage accounts per region per subscription: **250**
  * Maximum egress for general-purpose v2 and Blob storage accounts (all regions): **50 Gbps**
  * VMs per subscription per region: **25,000**
  * Resource groups per subscription: **980**
  

Due to security reasons, we also highly recommend separating the production and dev/stage environments into separate subscriptions.

# Note:

> ***It is important to divide your workspaces appropriately using different subscriptions based on your business keeping in mind the Azure limits.***

![Figure 3: Azure Databricks Isolation Domains Workspace](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure3.PNG "Figure 3: Azure Databricks Isolation Domains Workspace")

*Figure 3: Azure Databricks Isolation Domains Workspace*

**Note:** These limits are at a point in time and might change going forward.


## Consider Isolating Each Workspace in its own VNet
*Impact: Low*

While you can deploy more than one Workspace in a VNet by keeping the subnets separate, we recommend that you follow the hub and spoke model [hub and spoke model](https://en.wikipedia.org/wiki/Spoke%E2%80%93hub_distribution_paradigm) and separate each workspace in its own VNet.

Recall that a Databricks Workspace is designed to be a logical isolation unit, and that Azure’s VNets are designed for unconstrained connectivity among the resources placed inside it. Unfortunately, these two design goals are at odds with each other since VMs belonging to two different workspaces in the same
VNet can therefore communicate. While this is normally innocuous from our experience, it should be avoided if as much as possible.

**More information:** RFC 1918: Address allocation for private internets
RFC 1918: Address allocation for private internets [RFC 1918: Address allocation for private internets](https://tools.ietf.org/html/rfc1918)

![Figure 4: Hub and Spoke Model](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure4.PNG "Figure 4: Hub and Spoke Model")

*Figure 4: Hub and Spoke Model*

## Select the largest CIDR possible for a VNet
*Impact: High*

Recall the each Workspace can have multiple clusters:

  * Each cluster node requires 1 Public IP and 2 Private IPs
  * These IPs and are logically grouped into 2 subnets named “public” and “private”
  * For a desired cluster size of X, number of Public IPs = X, number of Private IPs = 4X
  * The 4X requirement for Private IPs is due to the fact that for each deployment:
    + Half of address space is reserved for future use
    + The other half is equally divided into the two subnets: private and public
  * The size of private and public subnets thur determines total number of VMs available for clusters 
    + /22 mask is larger than /23, so setting private and public to /22 will have more VMs available for creating clusters, than say /23 or below
   * But, because of the address space allocation scheme, the size of private and public subnets is constrained by the VNet’s CIDR
   * The allowed values for the enclosing VNet CIDR are from /16 through /24
   * The private and public subnet masks must be:
     + Equal
     + At least two steps down from enclosing VNet CIDR mask
     + Must be greater than /26
    
With this info, we can quickly arrive at the table below, showing how many nodes one can use across all clusters for a given VNet CIDR. It is clear that selection of VNet CIDR has far reaching implications in terms of maximum cluster size.   
    

![Table 1: CIDR ranges](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Table1.PNG "Table 1: CIDR ranges")

## Do not store any production data in default DBFS folders
*Impact: High*

This recommendation is driven by security and data availability concerns. Every Workspace comes with a default DBFS, primarily designed to store libraries and other system-level configuration artifacts such as Init scripts. You should not store any production data in it, because:
1. The lifecycle of default DBFS is tied to the Workspace. Deleting the workspace will also delete the default DBFS and permanently remove its contentents.
2. One can't restrict access to this default folder and its contents.


# Note:

> ***This recommendation doesn't apply to Blob or ADLS folders explicitly mounted as DBFS by the end user*** 

**More information: [Databricks File System]**(https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html)

## Always hide secrets in a Key Vault and do not expose them openly in Notebooks
*Impact: High*

It is a significant security risk to expose sensitive data such as access credentials openly in Notebooks or other places such as job configs, etc. You should instead use a vault to securely store and access them.
You can either use ADB’s internal Key Vault for this purpose or use Azure’s Key Vault (AKV) service.

If using Azure Key Vault, create separate AKV-backed secret scopes and corresponding AKVs to store credentials pertaining to different data stores. This will help prevent users from accessing credentials that they might not have access to. Since access controls are applicable to the entire secret scope, users with access to the scope will see all secrets for the AKV associated with that scope.

**More Information:**

[Create an Azure Key Vault-backed secret scope](https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html)

[Example of using secretin a notebook](https://docs.azuredatabricks.net/user-guide/secrets/example-secret-workflow.html)

[Best practices for creating secret scopes](https://docs.azuredatabricks.net/user-guide/secrets/secret-acl.html)







# Developing applications on ADB: Guidelines for selecting clusters

After understanding how to provision the workspaces, best practices in networking, etc., let’s put on the developer’s hat and see the design choices typically faced by them:

  * What type of clusters should I use?
  * How many drivers and how many workers?

In this chapter we will address such concerns and provide our recommendations, while also explaining the internals of Databricks clusters and associated topics. Some of these ideas seem counterintuitive but they will all make sense if you keep these important design attributes of the ADB service in mind:

1. **Cloud Optimized:** Azure Databricks is a product built exclusively for cloud environments, like Azure. No on-prem deployments currently exist. It assumes certain features are provided by the Cloud, is designed keeping Cloud best practices, and conversely, provides Cloud-friendly features.
2. **Platform/Software as a Service Abstraction:** ADB sits somewhere between the PaaS and SaaS ends of the spectrum, depending on how you use it. In either case ADB is designed to hide infrastructure details as much as possible so the user can focus on application development. It is
not, for example, an IaaS offering exposing the guts of the OS Kernel to you.
3. **Managed Service:** ADB guarantees a 99.95% uptime SLA. There’s a large team of dedicated staff members who monitor various aspects of its health and get alerted when something goes wrong. It is run like an always-on website and the staff strives to minimize any downtime.

These three attributes make ADB very different than other Spark platforms such as HDP, CDH, Mesos, etc. which are designed for on-prem datacenters and allow the user complete control over the hardware. The concept of a cluster is pretty unique in Azure Databricks. Unlike YARN or Mesos clusters which are just a collection of worker machines waiting for an application to be scheduled on them, clusters in ADB come with a pre-configured Spark application. ADB submits all subsequent user requests
like notebook commands, SQL queries, Java jar jobs, etc. to this primordial app for execution. This app is called the “Databricks Shell.”

Under the covers Databricks clusters use the lightweight Spark Standalone resource allocator. 

When it comes to taxonomy, ADB clusters are divided along notions of “type”, and “mode.” There are two ***types*** of ADB clusters, according to how they are created. Clusters created using UI are called Interactive Clusters, whereas those created using Databricks API are called Jobs Clusters. Further, each cluster can be of two ***modes***: Standard and High Concurrency. All clusters in Azure Databricks can automatically scale to match the workload, called Autoscaling.

*Table 2: Cluster modes and their characteristics*

![Table 2: Cluster modes and their characteristics](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Table2.PNG "Table 2: Cluster modes and their characteristics")


Support Interactive analytics using shared High Concurrency clusters
*Impact: Medium*

There are three steps for supporting Interactive workloads on ADB:
 1. Deploy a shared cluster instead of letting each user create their own cluster.
 2. Create the shared cluster in High Concurrency mode instead of Standard mode.
 3. Configure security on the shared High Concurrency cluster, using one of the following options:
     a. Turn on AAD Credential Passthrough if you’re using ADLS
     b. Turn on Table Access Control for all other stores

# Note:

> ***If you’re using ADLS, we currently recommend that you select either Table Access Control or AAD Credential Passthrough. Do not combine them together.*** 

To understand why, let’s quickly see how interactive workloads are different from batch workloads:

*Table 3: Batch vs. Interactive workloads*
![Table 3: Batch vs. Interactive workloads](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Table3.PNG "Table 3: Batch vs. Interactive workloads")

Because of these differences, supporting Interactive workloads entails minimizing cost variability and optimizing for latency over throughput, while providing a secure environment. These goals are satisfied by shared High Concurrency clusters with Table access controls or AAD Passthrough turned on (in case of ADLS):

  1. **Minimizing Cost:** By forcing users to share an autoscaling cluster you have configured with maximum node count, rather than say, asking them to create a new one for their use each time they log in, you can control the total cost easily. The max cost of shared cluster can be calculated by assuming it is running 24X7 at maximum size with the particular VMs. You can’t achieve this if each user is given free reign over creating clusters of arbitrary size and VMs.
  
  2. **Optimizing for Latency:** Only High Concurrency clusters have features which allow queries from different users share cluster resources in a fair, secure manner. HC clusters come with Query Watchdog, a process which keeps disruptive queries in check by automatically pre-empting rogue queries, limiting the maximum size of output rows returned, etc.
  
  3. **Security:** Table Access control feature is only available in High Concurrency mode and needs to be turned on so that users can limit access to their database objects (tables, views, functions...) created on the shared cluster. In case of ADLS, we recommend restricting access using the AAD Credential Passthrough feature instead of Table Access Controls.


![Figure 5: Interactive clusters](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure5.PNG "Figure 5: Interactive clusters")

*Figure 5: Interactive clusters*

That said, irrespective of the mode (Standard or High Concurrency), all Azure Databricks clusters use Spark Standalone cluster resource allocator and hence execute all Java and Scala user code in the same JVM. A shared cluster model is secure only for SQL or Python programs because:

  1. It is possible to isolate each user’s Spark SQL configuration storing sensitive credentials, temporary tables, etc. in a Spark Session. ADB creates a new Spark Session for each Notebook attached to a High Concurrency cluster. If you’re running SQL queries, then this isolation model works because there’s no way to examine JVM’s contents using SQL.
  2. Similarly, PySpark runs user queries in a separate process, so ADB can isolate DataFrames and DataSet operations belonging to different PySpark users.
  
In contrast a Scala or Java program from one user could easily steal secrets belonging to another user sharing the same cluster by doing a thread dump. Hence the isolation model of HC clusters, and this  recommendation, only applies to interactive queries expressed in SQL or Python. In practice this is rarely a limitation because Scala and Java languages are seldom used for interactive exploration. They are mostly used by Data Engineers to build data pipelines consisting of batch jobs. Those type of scenarios involve batch ETL jobs and are covered by the next recommendation.


# Support Batch ETL workloads with single user ephemeral Standard clusters
*Impact: Medium*

Unlike Interactive workloads, logic in batch Jobs is well defined and their cluster resource requirements are known a priori. Hence to minimize cost, there’s no reason to follow the shared cluster model and we
recommend letting each job create a separate cluster for its execution. Thus, instead of submitting batch ETL jobs to a cluster already created from ADB’s UI, submit them using the Jobs APIs. These APIs automatically create new clusters to run Jobs and also terminate them after running it. We call this the **Ephemeral Job Cluster** pattern for running jobs because the clusters short life is tied to the job lifecycle.

Azure Data Factory uses this pattern as well - each job ends up creating a separate cluster since the underlying call is made using the Runs-Submit Jobs API.


![Figure 6: Ephemeral Job cluster](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure6.PNG "Figure 6: Ephemeral Job cluster")

*Figure 6: Ephemeral Job cluster*

Just like the previous recommendation, this pattern will achieve general goals of minimizing cost, improving the target metric (throughput), and enhancing security by:

1. **Enhanced Security:** ephemeral clusters run only one job at a time, so each executor’s JVM runs code from only one user. This makes ephemeral clusters more secure than shared clusters for Java and Scala code.
2. **Lower Cost:** if you run jobs on a cluster created from ADB’s UI, you will be charged at the higher Interactive DBU rate. The lower Data Engineering DBUs are only available when the lifecycle of job and cluster are same. This is only achievable using the Jobs APIs to launch jobs on ephemeral clusters.
3. **Better Throughput:** cluster’s resources are dedicated to one job only, making the job finish faster than while running in a shared environment.

For very short duration jobs (< 10 min) the cluster launch time (~ 7 min) adds a significant overhead to total execution time. Historically this forced users to run short jobs on existing clusters created by UI -- a
costlier and less secure alternative. To fix this, ADB is coming out with a new feature called Warm Pools in Q3 2019 bringing down cluster launch time to 30 seconds or less.

## Favor Cluster Scoped Init Scripts over Global and Named scripts
*Impact: High*

Init Scripts provide a way to configure cluster’s nodes and can be used in the following modes:

  1. **Global:** by placing the init script in /databricks/init folder, you force the script’s execution every time any cluster is created or restarted by users of the workspace.
  2. **Cluster Named:** you can limit the init script to run only on for a specific cluster’s creation and restarts by placing it in /databricks/init/<cluster_name> folder.
  3. **Cluster Scoped:** in this mode the init script is not tied to any cluster by its name and its automatic execution is not a virtue of its dbfs location. Rather, you specify the script in cluster’s configuration by either writing it directly or providing its location on DBFS. Any location under DBFS /databricks folder except /databricks/init can be used for this purpose. eg, /databricks/<my-directory>/set-env-var.sh
 
You should treat Init scripts with extreme caution because they can easily lead to intractable cluster launch failures. If you *really* need them, a) try to use the Cluster Scoped execution mode as much as possible, and, b) write them directly in the cluster’s configuration rather than placing them on default DBFS and specifying the path. We say this because:

  1. ADB executes the script’s body in each cluster node’s LxC container before starting Spark’s executor or driver JVM in it -- the processes which ultimately run user code. Thus, a successful cluster launch and subsequent operation is predicated on all nodal init scripts executing in a timely manner without any errors and reporting a zero exit code. This process is highly error prone, especially for scripts downloading artifacts from an external service over unreliable and/or misconfigured networks.
  2. Because Global and Cluster Named init scripts execute automatically due to their placement in a special DBFS location, it is easy to overlook that they could be causing a cluster to not launch. By specifying the Init script in the Configuration, there’s a higher chance that you’ll consider them while debugging launch failures.
  3. As we explained earlier, all folders inside default DBFS are accessible to workspace users. Your init scripts containing sensitive data can be viewed by everyone if you place them there.
  
  
## Send logs to blob store instead of default DBFS using Cluster Log delivery
*Impact: Medium*

By default, Cluster logs are sent to default DBFS but you should consider sending the logs to a blob store location using the Cluster Log delivery feature. The Cluster Logs contain logs emitted by user code, as well as Spark framework’s Driver and Executor logs. Sending them to blob store is recommended over DBFS because:
  1. ADB’s automatic 30-day DBFS log purging policy might be too short for certain compliance scenarios. Blob store is the solution for long term log archival.
  2. You can ship logs to other tools only if they are present in your storage account and a resource group governed by you. The root DBFS, although present in your subscription, is launched inside a Microsoft-Azure Databricks managed resource group and is protected by a read lock. Because of this lock the logs are only accessible by privileged Azure Databricks framework code which shows them on UI. Constructing a pipeline to ship the logs to downstream log analytics tools requires logs to be in a lock-free location first.

## Choose cluster VMs to match workload class
*Impact: High*

To allocate the right amount and type of cluster resresource for a job, we need to understand how different types of jobs demand different types of cluster resources.

   * **Machine Learning** - To train machine learning models it’s usually required cache all of the data in memory. Consider using memory optimized VMs so that the cluster can take advantage of the RAM cache. To size the cluster, take a % of the data set → cache it → see how much memory it
used → extrapolate that to the rest of the data. The tungsten data serializer op􀆟mizes the data in-memory. Which means you’ll need to test the data to see the relative magnitude of compression.

  * **Streaming** - You need to make sure that the processing rate is just above the input rate at peak times of the day. Depending peak input rate times, consider compute optimized VMs for the cluster to make sure processing rate is higher than your input rate.
  
  * **ETL** - In this case, data size and deciding how fast a job needs to be will be a leading indicator. Spark doesn’t always require data to be loaded into memory in order to execute transformations, but you’ll at the very least need to see how large the task sizes are on shuffles and compare that to the task throughput you’d like. To analyze the performance of these jobs start with basics and check if the job is by CPU, network, or local I/O, and go from there. Consider using a general purpose VM for these jobs.
  * **Interactive / Development Workloads** - The ability for a cluster to auto scale is most important for these types of jobs. Azure Databricks has a cluster manager and Serverless clusters to optimize the size of cluster during peak and low times. In this case taking advantage of Serverless clusters and Autoscaling will be your best friend in managing the cost of the infrastructure.

## Arrive at correct cluster size by iterative performance testing
*Impact: High*

It is impossible to predict the correct cluster size without developing the application because Spark and Azure Databricks use numerous techniques to improve cluster utilization. The broad approach you should follow for sizing is:

  1. Develop on a medium sized cluster of 2-8 nodes, with VMs matched to workload class as explained earlier.
  
  2. After meeting functional requirements, run end to end test on larger representative data while measuring CPU, memory and I/O used by the cluster at an aggregate level.
  
  3. Optimize cluster to remove bottlenecks found in step 2:
         a. CPU bound: add more cores by adding more nodes
         b. Network bound: use fewer, bigger SSD backed machines to reduce             network size and improve remote read performance
         c. Disk I/O bound: if jobs are spilling to disk, use VMs with                 more memory.
         
   4. Repeat steps 2 and 3 by adding nodes and/or evaluating different VMs until all obvious bottlenecks have been addressed.
   
   
Performing these steps will help you to arrive at a baseline cluster size which can meet SLA on a subset of data. Because Spark workloads exhibit linear scaling, you can arrive at the production cluster size easily from here. For example, if it takes 5 nodes to meet SLA on a 100TB dataset, and the production data is around 1PB, then prod cluster is likely going to be around 50 nodes in size. 

## Tune shuffle for optimal performance
*Impact: High*

A shuffle occurs when we need to move data from one node to another in order to complete a stage. Depending on the type of transformation you are doing you may cause a shuffle to occur. This happens when all the executors require seeing all of the data in order to accurately perform the action. If the Job requires a wide transformation, you can expect the job to execute slower because all of the partitions need to be shuffled around in order to complete the job. Eg: Group by, Distinct.

![Figure 7: Ephemeral Job cluster](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure7.PNG "Figure 7: Ephemeral Job cluster")

*Figure 7: Shuffle vs. no-shuffle*


You’ve got two control knobs of a shuffle you can use to optimize
  * The number of partitions being shuffled:
    spark.conf.set("spark.sql.shuffle.partitions", 10) <--Priya to review HTML CSS later
  * The amount of partitions that you can compute in parallel.
        + This is equal to the number of cores in a cluster.

These two determine the partition size, which we recommend should be in the Megabytes to 1 Gigabyte range. If your shuffle partitions are too small, you may be unnecessarily adding more tasks to the stage. But if they are too big, you may get bottlenecked by the network.

## Store Data In Parquet Partitions
*Impact: High*

Azure Databricks has an optimized Parquet reader, enhanced over the Open Source Spark implementation and it is the recommended data storage format. In addition, storing data in partitions allows you to take advantage of partition pruning and data skipping. Most of the time partitions will be
on a date field but choose your partitioning field based on the relevancy to the queries the data is supporting. For example, if you’re always going to be filtering based on “Region,” then consider partitioning your data by region.
   * Evenly distributed data across all partitions (date is the most common)
   * 10s of GB per partition (~10 to ~50GB)
   * Small data sets should not be partitioned
   * Beware of over partitioning
   

# Monitoring

Once you have your clusters setup and your Spark applications running, there is a need to monitor your Azure Databricks pipelines. These pipelines are rarely executed in isolation and need to be monitored
along with a set of other services. Monitoring falls into four broad areas:

   (1) Resource utilization (CPU/Memory/Network) across an Azure        Databricks cluster. This is referred to as VM metrics
   (2) Spark metrics which enables monitoring of Spark applications to      help uncover bottlenecks 
   (3) Spark application logs which enables administrators/developers to query the logs, debug issues and investigate job run failures. This is specifically helpful to also understand exceptions across your workloads
   (4) Application instrumentation which is native instrumentation that you add to your application for custom troubleshooting

For the purposes of this version of this document we will focus on (1). This is the most common ask from customers.

## Collect resource utilization metrics across Azure Databricks cluster in a Log Analytics workspace
*Impact: Medium*

An important facet of monitoring is understanding the resource utilization across an Azure Databricks cluster. You can also extend this to understand utilization across all your Azure Databricks clusters in a workspace. This could be useful in arriving at a cluster size and VM sizes given each VM size does have a set of limits (cores/disk throughput/network throughput) and could play a role in the performance profile of an Azure Databricks job.
In order to get utilization metrics of the Azure Databricks cluster, we use the Azure Linux diagnostic extension as an init script into the clusters we want to monitor. Note: This could increase your cluster
startup time by a minute.

You can use the instructions in Appendix A to install the Log Analytics agent on Azure Databricks agent to collect VM metrics in your Log Analytics workspace.

### Querying VM metrics in Log Analytics once you have started the collection using the above document

#### Sub-sub-heading

This is an h3 heading

## Heading

This is an h1 heading

### Sub-heading

This is an h2 heading

#### Sub-sub-heading

This is an h3 heading

## Heading

This is an h1 heading

### Sub-heading

This is an h2 heading

#### Sub-sub-heading

This is an h3 heading
