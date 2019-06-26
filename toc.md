# Table of Contents

- [Introduction](#Introduction)
- [Provisioning ADB: Guidelines for Networking and Security](#ProvisioningADB:GuidelinesforNetworkingandSecurity)
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

Planning, deploying, and running Azure Databricks (ADB) at scale requires one to make many
architectural decisions.

While each ADB deployment is unique to an organization's needs we have found that some patterns are
common across most successful ADB projects. Unsurprisingly, these patterns are also in-line with
modern Cloud-centric development best practices.

This short guide summarizes these patterns into prescriptive and actionable best practices for Azure
Databricks. We follow a logical path of planning the infrastructure, provisioning the workspaces,
developing Azure Databricks applications, and finally, running Azure Databricks in production.

The audience of this guide are system architects, field engineers, and development teams of customers,
Microsoft, and Databricks. Since the Azure Databricks product goes through fast iteration cycles, we
have avoided recommendations based on roadmap or Private Preview features.

Our recommendations should apply to a typical Fortune 500 enterprise with at least intermediate level
of Azure and Databricks knowledge. We've also classified each recommendation according to its likely
impact on solution's quality attributes. Using the **Impact** factor, you can weigh the recommendation
against other competing choices. Example: if the impact is classified as “Very High”, the implications of
not adopting the best practice can have a significant impact on your deployment.

As ardent cloud proponents, we value agility and bringing value quickly to our customers. Hence, we’re
releasing the first version somewhat quickly, omitting some important but advanced topics in the
interest of time. We will cover the missing topics and add more details in the next round, while sincerely
hoping that this version is still useful to you.

## Provisioning ADB: Guidelines for Networking and Security

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
