from ocs_sample_library_preview import OCSClient
import configparser

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
config = configparser.ConfigParser()
config.read('config.ini')

source_sds_source = OCSClient(config.get('SourceConfiguration', 'ApiVersion'),
                              config.get(
                                  'SourceConfiguration', 'TenantId'),
                              config.get(
                                  'SourceConfiguration', 'Resource'),
                              config.get(
                                  'SourceConfiguration', 'ClientId'),
                              config.get('SourceConfiguration', 'ClientSecret'))
source_sds_source.request_timeout = request_timeout

destination_sds_source = OCSClient(config.get('DestinationConfiguration', 'ApiVersion'),
                                   config.get(
    'DestinationConfiguration', 'TenantId'),
    config.get(
    'DestinationConfiguration', 'Resource'),
    config.get(
    'DestinationConfiguration', 'ClientId'),
    config.get('DestinationConfiguration', 'ClientSecret'))
destination_sds_source.request_timeout = request_timeout

source_namespace_id = config.get('SourceConfiguration', 'NamespaceId')
destination_namespace_id = config.get(
    'DestinationConfiguration', 'NamespaceId')
