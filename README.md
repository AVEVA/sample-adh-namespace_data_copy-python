# OSIsoft Cloud Services Namespace Data Copy Python Sample

**Version:** 1.0.2

[![Build Status](https://dev.azure.com/osieng/engineering/_apis/build/status/product-readiness/OCS/osisoft.sample-ocs-namespace_data_copy-python?repoName=osisoft%2Fsample-ocs-namespace_data_copy-python&branchName=main)](https://dev.azure.com/osieng/engineering/_build/latest?definitionId=3856&repoName=osisoft%2Fsample-ocs-namespace_data_copy-python&branchName=main)

Developed against Python 3.9.1.

## Requirements

- Python 3.9+
- Register a [Client-Credentials Client](https://cloud.osisoft.com/clients) in your OSIsoft Cloud Services tenant and create a client secret to use in the configuration of this sample. ([Video Walkthrough](https://www.youtube.com/watch?v=JPWy0ZX9niU))
- Install required modules: `pip install -r requirements.txt`

## About this sample

This sample uses REST API calls to copy data views, assets, and streams from a source namespace to a destination namespace. Some common use cases are described in the [Configuring the sample](#configuring-the-sample) section. The steps are as follows

1. Copy the data view specified by its Id and add all referenced assets and streams to lists to be copied later
1. Add all streams referenced by assets found by the specified query and referenced by the previously found data view to a list to be copied later
1. Copy all streams found by the specified query and referenced by the previously found assets and data view
1. Copy all assets found by the specified query and referenced by the previously found data view
1. If testing, cleanup all created data views, assets, and streams

## Configuring the sample

The sample is configured by modifying the files [config.py](config.py) and [config.placeholder.ini](config.placeholder.ini). Details on how to configure them can be found in the sections below. Before editing config.placeholder.ini, rename this file to `config.ini`. This repository's `.gitignore` rules should prevent the file from ever being checked in to any fork or branch, to ensure credentials are not compromised.

### Configuring config.py

The configurable parameters within config.py are described in the table below. In addition, the common use cases for this sample and how to configure parameters are described in the following subsections.

| Parameters       | Description                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| stream_query     | The query used to find streams to copy. This can be set to '\*' to copy all streams or None to copy no streams (besides the underlying streams of any queried assets or the specified data view). Please refer to [docs.osisoft.com](https://docs.osisoft.com/bundle/ocs/page/api-reference/sequential-data-store/sds-search.html) for more information on searching for streams. |
| asset_query      | The query used to find assets to copy. This can be set to '\*' to copy all assets or None to copy no assets (besides the underlying assets of the specified data view). Please refer to [docs.osisoft.com](https://docs.osisoft.com/bundle/ocs/page/api-reference/assets/asset-search-api.html) for more information on searching for assets.                                     |
| data_view_id     | The id of the data view to copy.                                                                                                                                                                                                                                                                                                                                                  |
| prefix           | The prefix to apply to the Ids of all streams, assets, and dataviews along with all underlying types.                                                                                                                                                                                                                                                                             |
| max_stream_count | The maximum number of streams to be returned by the stream query. The maximum value is 1,000.                                                                                                                                                                                                                                                                                     |
| max_asset_count  | The maximum number of assets to be returned by the stream query. The maximum value is 1,000.                                                                                                                                                                                                                                                                                      |
| start_time       | The start time of the data window to be transfered.                                                                                                                                                                                                                                                                                                                               |
| end_time         | The end time of the data window to be transfered.                                                                                                                                                                                                                                                                                                                                 |
| request_timeout  | The time before a request time-out. If requests are timing-out try increasing this value.                                                                                                                                                                                                                                                                                         |

#### Copying all streams

1. Set max_stream_count to 1,000 (if more than 1,000 streams are being copied, the copying will need to be done in batches)
1. Set stream_query to '\*' (if copying in batches, this query will need to be modified)
1. Set asset_query to None
1. Set data_view_id to None

#### Copying all assets and all underlying streams

1. Set max_asset_count to 1,000 (if more than 1,000 assets are being copied, the copying will need to be done in batches)
1. Set stream_query to None
1. Set asset_query to '\*' (if copying in batches, this query will need to be modified)
1. Set data_view_id to None

#### Copy a data view and all underlying assets and streams

1. Set stream_query to None
1. Set asset_query to None
1. Set data_view_id to the desire data_view_id

### Configuring config.ini

OSIsoft Cloud Services is secured by obtaining tokens from its identity endpoint. Client credentials clients provide a client application identifier and an associated secret (or key) that are authenticated against the token endpoint. You must replace the placeholders in your `config.ini` file with the authentication-related values from your tenant and a client-credentials client created in your OCS tenant. There are separate configurations for both the source and destination namespaces; however, there can be overlap in some settings such as TenantId, ClientId, ClientSecret, and even NamespaceId if, for example, you want to migrate to a namespace in the same tenant.

```ini
[SourceConfiguration]
NamespaceId = REPLACE_WITH_CURRENT_NAMESPACE_ID
TenantId = REPLACE_WITH_CURRENT_TENANT_ID
ClientId = REPLACE_WITH_CURRENT_CLIENT_ID
ClientSecret = REPLACE_WITH_CURRENT_CLIENT_SECRET
Resource = https://dat-b.osisoft.com
ApiVersion = v1

[DestinationConfiguration]
NamespaceId = REPLACE_WITH_NEW_NAMESPACE_ID
TenantId = REPLACE_WITH_NEW_TENANT_ID
ClientId = REPLACE_WITH_NEW_CLIENT_ID
ClientSecret = REPLACE_WITH_NEW_CLIENT_SECRET
Resource = https://dat-b.osisoft.com
ApiVersion = v1
```

## Running the sample

To run this example from the command line once the `config.ini` is configured, run

```shell
python program.py
```

## Running the automated test

**Note: The test will delete the Data View, Assets, and Streams that were created.**

To test the sample, run

```shell
pip install pytest
python -m pytest test.py
```

---

Tested against Python 3.9.1

For the main OCS samples page [ReadMe](https://github.com/osisoft/OSI-Samples-OCS)  
For the main OSIsoft samples page [ReadMe](https://github.com/osisoft/OSI-Samples)
