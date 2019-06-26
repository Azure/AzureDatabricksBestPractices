# Table of Contents

- [Introduction](#Introduction)
- [Provisioning ADB: Guidelines for Networking and Security](#Provisioning-ADB-Guidelines-for-Networking-and-Security)
  * [Azure Databricks 101](#sub-heading-1)
  * [Map Workspaces to Business Units](#sub-heading-1)
  * [Deploy Workspaces in Multiple Subscriptions](#sub-heading-1)
    + [ADB Workspace Limits](#sub-sub-heading-1)
    + [Azure Subscription Limits](#sub-sub-heading-1)
  * [Consider Isolating Each Workspace in its own VNet](#sub-heading-1)
  * [Select the largest CIDR possible for a VNet](#sub-heading-1)
  * [Do not store any production data in default DBFS folders](#sub-heading-1)
  * [Always hide secrets in Key Vault and do not expose them openly in Notebooks](#sub-heading-1)
- [Developing applications on ADB: Guidelines for selecting clusters](#heading-2)
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

● Networking requirements of Databricks

● The number and the type of Azure networking resources required to launch clusters

● Relationship between Azure and Databricks jargon: Subscription, VNet., Workspaces, Clusters, Subnets, etc.

● Overall Capacity Planning process: where to begin, what to consider?
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

## Heading

This is an h1 heading

### Sub-heading

This is an h2 heading

#### Sub-sub-heading

This is an h3 heading
