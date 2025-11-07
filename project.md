**Azure Blob storage configuration**

Basics
Subscription
Azure subscription 1
Resource group
storage
Location
Germany West Central
Storage account name
mattamplitude2025
Preferred storage type
Azure Blob Storage or Azure Data Lake Storage Gen 2
Primary workload
Performance
Standard
Replication
Locally-redundant storage (LRS)
Advanced
Enable hierarchical namespace
Disabled
Enable SFTP
Disabled
Enable network file system v3
Disabled
Allow cross-tenant replication
Disabled
Access tier
Hot
Enable large file shares
Enabled
Security
Secure transfer
Enabled
Blob anonymous access
Disabled
Allow storage account key access
Enabled
Default to Microsoft Entra authorization in the Azure portal
Disabled
Minimum TLS version
Version 1.2
Permitted scope for copy operations (preview)
From any storage account
Networking
Public network access
Enabled
Public network access scope
Enabled from all networks
Default routing tier
Microsoft network routing
Data protection
Point-in-time restore
Disabled
Blob soft delete
Enabled
Blob retainment period in days
7
Container soft delete
Enabled
Container retainment period in days
7
File share soft delete
Enabled
File share retainment period in days
7
Versioning
Disabled
Blob change feed
Disabled
Version-level immutability support
Disabled
Encryption
Encryption type
Microsoft-managed keys (MMK)
Enable support for customer-managed keys
Blobs and files only
Enable infrastructure encryption
Disabled


**Airbyte Source Configuration**

{
  "name": "Amplitude",
  "workspaceId": "35164211-568b-4a8d-8f0d-5dd263a75c72",
  "definitionId": "fa9f58c6-2d03-4237-aaa4-07d75e0c1396",
  "configuration": {
    "api_key": "******",
    "secret_key": "******",
    "start_date": "2025-10-30T00:00:00Z",
    "data_region": "EU Residency Server",
    "request_time_range": 24,
    "active_users_group_by_country": true
  }
}


**Airbyte Destination Configuration**


{
  "name": "Azure Blob Storage mattamplitude2025",
  "workspaceId": "35164211-568b-4a8d-8f0d-5dd263a75c72",
  "definitionId": "b4c5d105-31fd-4817-96b6-cb923bfc04cb",
  "configuration": {
    "format": {
      "flattening": "No flattening",
      "format_type": "JSONL"
    },
    "azure_blob_storage_spill_size": 500,
    "azure_blob_storage_account_name": "mattamplitude2025",
    "azure_blob_storage_account_key": "******",
    "azure_blob_storage_container_name": "amplitude"
  }
}


**Amplitude Export API Endpoint**
https://analytics.eu.amplitude.com/api/2/export


**Amplitude Export API Official Documentation**
https://amplitude.com/docs/apis/analytics/export


**Extracting from Amplitude with Python**

Extracting with Python

Amplitude's Export API

The Export API lets you export event data, i.e. user interactions on the website.

Documentation: https://amplitude.com/docs/apis/analytics/export
Authentication: Basic, e.g.
--header 'Authorization: Basic YWhhbWwsdG9uQGFwaWdlZS5jb206bClwYXNzdzByZAo'
Endpoint: https://analytics.eu.amplitude.com/api/2/export (EU residency server)
Review Considerations
Review Response Schema
Pairs Task

In pairs plan how you would write your Python scripts to extract the data from this API. This does not need to be perfect code, in fact it should be "pseudo" code - a plain english description of the steps you'd like to take.

For example:

Create Python file
Check Python version
Decide what libraries to use
etc.
The cURL Request

curl --location --request GET 'https://analytics.eu.amplitude.com/api/2/export?start=<starttime>&end=<endtime>' \
-u '{api_key}:{secret_key}'
Translating cURL to Python Requests

--location: This tells cURL to follow redirects. The requests library handles this automatically.
--request GET: Specifies the HTTP method. In requests, this translates to using requests.get().
The URL: 'https://analytics.eu.amplitude.com/api/2/export?start=&end=' is passed as an argument to requests.get().
-u '{api_key}:{secret_key}': This specifies Basic Authentication, which can be handled using the auth parameter in requests.
An example of the cURL call using the requests library

import requests

# API endpoint is the EU residency server
url = 'https://analytics.eu.amplitude.com/api/2/export'
params = {
    'start': start_time,
    'end': end_time
}

# Make the GET request with basic authentication
response = requests.get(url, params=params, auth=(api_key, secret_key))

