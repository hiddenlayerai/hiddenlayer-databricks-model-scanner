# HiddenLayer Databricks Model Scanner

HiddenLayer’s Model Scanner integrates with Databricks to automatically scan ML models for vulnerabilities. The autoscan installer that is packaged in this repo will install the necessary notebooks and configure jobs to automate the scanning for you.

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
- Personal Access Token (PAT) - Used to authenticate access to Databricks resources. Link to the Databricks PAT documentation.
- Catalog(s) - The name of the Unity Catalog to scan.
- Schema(s) - The schema the models are registered in.
- Compute - The ID for the cluster running the jobs; must have UC access.

You will need the following information for Hiddenlayer, whican can be obtained from the console:
- Client ID - HiddenLayer API Client ID.
- Client Secret - HiddenLayer API Client Secret.

The CLI is run via `hldbx autoscan`

The CLI can be configured via a [configuration file](#configuration-file). If a configuration file is not provided, the installer will prompt for necessary information.

## Support Products

The Databricks autoscan is capable of interfacing with Hiddenlayer's Saas Model Scanner offering as well as the On-Premise Enterprise Model Scanner. Configuration will default to The Saas offering unless the URL for the Enterprise Model Scanner is provided.

## Configuration File

The Databricks autoscan can be driven by a yaml config file. This file should be placed at $HOME/.hl/hldbx.yaml.

An example configuration can be found at [config_template.yaml](https://github.com/hiddenlayerai/hiddenlayer-databricks-model-scanner/blob/main/config_template.yaml)
