# HiddenLayer Databricks Model Scanner

HiddenLayer’s Model Scanner integrates with Databricks to automatically scan ML models for vulnerabilities. The autoscan installer that is packaged in this repo will install the necessary notebooks and configure scheduled jobs to automate the discovery and scanning for you.

## Contents

- [Installation](#installation)
- [Getting started](#getting-started)
- [CLI](#cli)
- [Configuraton File](#configuration-file)

## Installation

Retrieve the latest version of the CLI from our releases page.

## Getting Started

You will need the following information for Databricks:
- URL - The workspace URL for your Databricks instance.
- Personal Access Token (PAT) - Used to authenticate access to Databricks resources for notebook install and scheduled job creation
- Catalog(s) - The name of the Unity Catalog to scan.
- Schema(s) - The schema the models are registered in.
- Compute - The ID for the cluster running the jobs; must have UC access.

[!NOTE]
> The PAT is used to install the notebooks in your environment and setup the jobs. It is not necessarily the context that the jobs will run as.

Optionally you may choose to have the jobs run in the context of a Service Principal. If you choose this option you will need:
- ID of the Service Principal

You will need the following information for Hiddenlayer, which can be obtained from the console:
- Client ID - HiddenLayer API Client ID.
- Client Secret - HiddenLayer API Client Secret.

The CLI is run via `hldbx autoscan`

The CLI can be configured via a [configuration file](#configuration-file). If a configuration file is not provided, the installer will prompt for necessary information.

## Supported Products

The Databricks autoscan is capable of interfacing with Hiddenlayer's Saas Model Scanner as well as the On-Premise Enterprise Model Scanner. Configuration will default to the Saas offering unless the URL for an Enterprise Model Scanner is provided.

## Configuration File

The Databricks autoscan can be driven by a yaml config file. This file should be placed at $HOME/.hl/hldbx.yaml.

An example configuration can be found at [config_template.yaml](https://github.com/hiddenlayerai/hiddenlayer-databricks-model-scanner/blob/main/config_template.yaml)
