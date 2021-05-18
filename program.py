from ocs_sample_library_preview.DataView.DataItemResourceType import DataItemResourceType
from ocs_sample_library_preview.SDS.SdsResultPage import SdsResultPage
from ocs_sample_library_preview import OCSClient
import configparser
import json

# Settings
stream_query = 'ffffffffffff'#f'"SLTCSensorUnit2"'
asset_query = 'fffffffffffff'#f'"SLTC AC Unit 2"'
data_view_id = 'ABC:TankView'#f'SLTC Sensor Unit'
prefix = ''  # Customer prefix for streamId
max_stream_count = 150  # The maximum number of streams to copy
max_asset_count = 150  # The maximum number of assets to copy
max_data_view_count = 150  # The maximum number of streams to copy
start_time = '2021-05-16'  # Time window of values to transfer
end_time = '2021-05-18'


def copyStream(stream, current_sds_source, new_sds_source, start_time, end_time, prefix=''):
    # Ensure type exists in new namespace
    new_type = current_sds_source.Types.getType(
        namespace_id=current_namespace_id, type_id=stream.TypeId)
    new_type.Id = f"{prefix}{stream.TypeId}"  # optional
    new_sds_source.Types.getOrCreateType(
        namespace_id=new_namespace_id, type=new_type)

    # Create the new stream
    current_stream_id = stream.Id
    stream.Id = f"{prefix}{stream.Id}"  # optional
    stream.TypeId = new_type.Id
    new_sds_source.Streams.getOrCreateStream(
        namespace_id=new_namespace_id, stream=stream)

    # Copy the values from the current stream to the new stream
    data = SdsResultPage()
    while not data.end():
        data = current_sds_source.Streams.getWindowValuesPaged(
            namespace_id=current_namespace_id, stream_id=current_stream_id, value_class=None,
            start=start_time, end=end_time, count=250000, continuation_token=data.ContinuationToken)
        if len(data.Results) > 0:
            new_sds_source.Streams.updateValues(
                namespace_id=new_namespace_id, stream_id=stream.Id, values=json.dumps(data.Results))


def copyAsset(asset, current_sds_source, new_sds_source, start_time, end_time, prefix=''):
    # Copy over referenced streams
    for stream_reference in asset.StreamReferences:
        stream = current_sds_source.Streams.getStream(
            namespace_id=current_namespace_id, stream_id=stream_reference.StreamId)
        copyStream(stream, current_sds_source,
                   new_sds_source, start_time, end_time)

    # Ensure type exists in new namespace
    if asset.AssetTypeId != None:
        new_asset_type = current_sds_source.Assets.getAssetTypeById(
            namespace_id=current_sds_source, asset_type_id=asset.AssetTypeId)
        new_asset_type.Id = f"{prefix}{asset.AssetTypeId}"
        new_sds_source.Assets.createOrUpdateAssetType(
            namespace_id=new_sds_source, asset_type=new_asset_type)

    # Create the new asset
    new_asset = current_sds_source.Assets.getAssetById(
        namespace_id=current_namespace_id, asset_id=asset.Id)
    new_asset.Id = f"{prefix}{asset.Id}"
    new_sds_source.Assets.createOrUpdateAsset(
        namespace_id=new_namespace_id, asset=new_asset)


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
streams = current_sds_source.Streams.getStreams(
    namespace_id=current_namespace_id, query=stream_query, skip=0, count=max_stream_count)

# For each stream found, copy over the type, create the new stream, and copy the values
for stream in streams:

    copyStream(stream, current_sds_source,
               new_sds_source, start_time, end_time)

# Find all assets via a query (repeat script is multiple searches are necessary)
assets = current_sds_source.Assets.getAssets(
    namespace_id=current_namespace_id, query=asset_query, skip=0, count=max_asset_count)

# For each asset found, copy the referenced streams, copy the values, copy over the type, and create the new asset
for asset in assets:

    copyAsset(asset, current_sds_source,
              new_sds_source, start_time, end_time)

current_sds_source = OCSClient("v1",
                               config.get('CurrentConfiguration', 'TenantId'),
                               config.get('Access', 'Resource'),
                               config.get('CurrentConfiguration', 'ClientId'),
                               config.get('CurrentConfiguration', 'ClientSecret'))

new_sds_source = OCSClient("v1",
                           config.get('NewConfiguration', 'TenantId'),
                           config.get('Access', 'Resource'),
                           config.get('NewConfiguration', 'ClientId'),
                           config.get('NewConfiguration', 'ClientSecret'))

# Find the data view with the specified Id
current_sds_source
data_view = current_sds_source.DataViews.getDataView(
    namespace_id=current_namespace_id, data_view_id=data_view_id)

# Copy all assets or streams that are referenced by the data view
for query in data_view.Queries:

    if query.Kind == DataItemResourceType.Stream:
        streams = current_sds_source.Streams.getStreams(
            namespace_id=current_namespace_id, query=stream_query, skip=0, count=max_stream_count)
        for stream in streams:
            copyStream(stream, current_sds_source,
                    new_sds_source, start_time, end_time)
            

    elif query.Kind == DataItemResourceType.Assets:
        assets = current_sds_source.Assets.getAssets(
            namespace_id=current_namespace_id, query=query.Value, skip=0, count=max_asset_count)
        for asset in assets:
            copyAsset(asset, current_sds_source,
                      new_sds_source, start_time, end_time)
    else:
        raise ValueError

# Create the data view
new_sds_source.DataViews.putDataView(namespace_id=new_namespace_id, data_view=data_view)