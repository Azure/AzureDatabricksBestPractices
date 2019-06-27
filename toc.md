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
  * [Support Interactive analytics using shared High Concurrency clusters](#sub-heading-2)
   * [Support Batch ETL workloads with single user ephemeral Standard clusters](#sub-heading-2)
   * [Favor Cluster Scoped Init scripts over Global and Named scripts](#sub-heading-2)
   * [Send logs to blob store instead of default DBFS using Cluster Log delivery](#sub-heading-2)
   * [Choose cluster VMs to match workload class](#sub-heading-2)
   * [Arrive at correct cluster size by iterative performance testing](#sub-heading-2)
   * [Tune shuffle for optimal performance](#sub-heading-2)
   * [Store Data In Parquet Partitions](#sub-heading-2)
    + [Sub-sub-heading](#sub-sub-heading-2)
- [Monitoring](#heading)
  * [Collect resource utilization metrics across Azure Databricks cluster in a Log Analytics workspace](#sub-heading)
- [Appendix A](#heading)
  * [Installation for being able to capture VM metrics in Log Analytics](#sub-heading)


   
    
    
    
    
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

Azure Databricks comes with its own user management interface. You can create users and groups in a workspace, assign them certain privileges, etc. While users in AAD are equivalent to Databricks users, by default AAD roles have no relationship with groups created inside ADB. ADB also has a special group
called Admin, not to be confused with AAD’s admin.

The first user to login and initialize the workspace is the workspace ***owner***. This person can invite other users to the workspace, create groups, etc. The ADB logged in user’s identity is provided by AAD, and shows up under the user menu in Workspace:

![Figure 1: Databricks user menu](https://github.com/Azure/AzureDatabricksBestPractices/blob/master/Figure1.PNG "Figure 1: Databricks user menu")


*Figure 1: Databricks user menu*

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
