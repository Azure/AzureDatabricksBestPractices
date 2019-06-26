# Table of Contents

- [Introduction](#heading)
  * [Sub-heading](#sub-heading)
    + [Sub-sub-heading](#sub-sub-heading)
- [Provisioning ADB: Guidelines for Networking and Security](#heading-1)
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

- [Introduction](#heading)
  * [Sub-heading](#sub-heading)
    + [Sub-sub-heading](#sub-sub-heading)
    
# Table of Tables

- [Introduction](#heading)
  * [Sub-heading](#sub-heading)
    + [Sub-sub-heading](#sub-sub-heading)

# Heading levels

> This is a fixture to test heading levels

<!-- toc -->

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

## Heading

This is an h1 heading

### Sub-heading

This is an h2 heading

#### Sub-sub-heading

This is an h3 heading
