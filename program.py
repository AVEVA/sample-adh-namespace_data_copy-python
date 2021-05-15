from ocs_sample_library_preview import OCSClient
import configparser
import json

# Settings
stream_query = f'kduffy_OpenWeather_*'
prefix = 'abc:'  # Customer prefix for streamId
starttime = '2019-01-01'  # Time window of values to transfer
endtime = '2021-05-12'

# Configuration
config = configparser.ConfigParser()
config.read('config.ini')

current_sds_source = OCSClient(config.get('Access', 'ApiVersion'),
                      config.get('CurrentConfiguration', 'TenantId'),
                      config.get('Access', 'Resource'),
                      config.get('CurrentConfiguration', 'ClientId'),
                      config.get('CurrentConfiguration', 'ClientSecret'))

new_sds_source = OCSClient(config.get('Access', 'ApiVersion'),
                      config.get('NewConfiguration', 'TenantId'),
                      config.get('Access', 'Resource'),
                      config.get('NewConfiguration', 'ClientId'),
                      config.get('NewConfiguration', 'ClientSecret'))

current_namespace_id = config.get('CurrentConfiguration', 'NamespaceId')
new_namespace_id = config.get('NewConfiguration', 'NamespaceId')


# Find all streams via a query (repeat script is multiple searches are necessary)
streams = current_sds_source.Streams.getStreams(namespace_id=current_namespace_id, query=stream_query, skip=0, count=150)

# For each stream found, copy over the type, create the new stream, and copy the values
for stream in streams:
    
    # Ensure type exists in new namespace
    new_type = current_sds_source.Types.getType(namespace_id=current_namespace_id, type_id=stream.TypeId)
    new_type.Id = f"{prefix}{stream.TypeId}" # optional
    new_sds_source.Types.getOrCreateType(namespace_id=new_namespace_id, type=new_type)

    # Create the new stream
    current_stream_id = stream.Id
    stream.Id = f"{prefix}{stream.Id}" # optional
    stream.TypeId = new_type.Id
    new_sds_source.Streams.getOrCreateStream(namespace_id=new_namespace_id, stream=stream)

    # Copy the values from the current stream to the new stream
    data = current_sds_source.Streams.getWindowValues(
        namespace_id=current_namespace_id, stream_id=current_stream_id, value_class=None, start=starttime, end=endtime)
    if len(data) > 0:
        new_sds_source.Streams.updateValues(
            namespace_id=new_namespace_id, stream_id=stream.Id, values=json.dumps(data))

