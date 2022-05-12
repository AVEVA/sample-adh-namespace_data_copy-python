from adh_sample_library_preview import ADHClient
import json

# Settings
stream_query = '"SLTCSensorUnit1"'
asset_query = '"SLTC Sensor1"'
data_view_id = 'SLTC Sensor Unit'
prefix = ''  # prefix for streams, assets, etc.
test_prefix = 'SAMPLE_TEST:'  # prefix for testing
max_stream_count = 150  # The maximum number of streams to copy
max_asset_count = 150  # The maximum number of assets to copy
start_time = '2021-05-01'  # Time window of values to transfer
end_time = '2021-05-02'
request_timeout = 30  # The request timeout limit

# Read configuration
try:
    with open(
        'appsettings.json',
        'r',
    ) as f:
        appsettings = json.load(f)
except Exception as error:
    print(f'Error: {str(error)}')
    print(f'Could not open/read appsettings.json')
    exit()

source_sds_source = ADHClient(appsettings.get('SourceConfiguration').get('ApiVersion'),
                              appsettings.get('SourceConfiguration').get('TenantId'),
                              appsettings.get('SourceConfiguration').get('Resource'),
                              appsettings.get('SourceConfiguration').get('ClientId'),
                              appsettings.get('SourceConfiguration').get('ClientSecret'))
source_sds_source.request_timeout = request_timeout

destination_sds_source = ADHClient(appsettings.get('DestinationConfiguration').get('ApiVersion'),
                                   appsettings.get('DestinationConfiguration').get('TenantId'),
                                   appsettings.get('DestinationConfiguration').get('Resource'),
                                   appsettings.get('DestinationConfiguration').get('ClientId'),
                                   appsettings.get('DestinationConfiguration').get('ClientSecret'))
destination_sds_source.request_timeout = request_timeout

source_namespace_id = appsettings.get('SourceConfiguration').get('NamespaceId')
destination_namespace_id = appsettings.get('DestinationConfiguration').get('NamespaceId')
