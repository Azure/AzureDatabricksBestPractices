# Databricks notebook source
# Config for AzureML Workspace
# for secure keeping, store credentials in Azure Key Vault and link using Azure Databricks secrets with dbutils
subscription_id = "" 
resource_group = ""                       
workspace_name = ""                         

tenant_id = "" # Tenant ID
sp_id = "" # Service Principal ID
sp_secret = "" # Service Principal Secret