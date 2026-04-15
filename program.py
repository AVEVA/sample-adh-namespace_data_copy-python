import gzip

from adh_sample_library_preview import DataItemResourceType, ADHClient, SdsResultPage
from concurrent.futures import ThreadPoolExecutor
import traceback
import json

from fastapi import requests
import appsettings


def copyTypeByTypeId(type_id, source_sds_source, source_namespace_id, destination_sds_source, destination_namespace_id, prefix=''):

    # Create the new type
    destination_type = source_sds_source.Types.getType(
        namespace_id=source_namespace_id, type_id=type_id)
    destination_type.Id = f'{prefix}{destination_type.Id}'  # optional
    destination_sds_source.Types.getOrCreateType(
        namespace_id=destination_namespace_id, type=destination_type)


def copyStream(stream, source_sds_source: ADHClient, source_namespace_id, destination_sds_source: ADHClient, destination_namespace_id, start_time, end_time, prefix=''):

    # Create the new stream
    source_stream_id = stream.Id
    stream.Id = f'{prefix}{stream.Id}'  # optional
    stream.TypeId = f'{prefix}{stream.TypeId}'  # optional
    destination_sds_source.Streams.getOrCreateStream(
        namespace_id=destination_namespace_id, stream=stream)

    # Copy the values from the current stream to the new stream
    data = source_sds_source.Streams.getWindowValuesPaged(
        namespace_id=source_namespace_id, stream_id=source_stream_id, value_class=None,
        start=start_time, end=end_time, count=1000)
    if len(data.Results) > 0:
        destination_sds_source.Streams.updateValues(
            namespace_id=destination_namespace_id, stream_id=stream.Id, values=json.dumps(data.Results))

    while not data.end():
        data = source_sds_source.Streams.getWindowValuesPaged(
            namespace_id=source_namespace_id, stream_id=source_stream_id, value_class=None,
            start=start_time, end=end_time, count=250000, continuation_token=data.ContinuationToken)
        if len(data.Results) > 0:
            destination_sds_source.Streams.updateValues(
                namespace_id=destination_namespace_id, stream_id=stream.Id, values=json.dumps(data.Results))

def prepareDataOmfMessageGzipContent(sdsData: SdsResultPage, stream_id):
    # Step 1. construct omf data json message 
    payload_dicts = []
    payload_dicts.append({
        "ContainerId": stream_id,
        "Values": sdsData.Results
    })

    # Step 2. serialize json and gzip compress the result
    json_bytes = json.dumps(payload_dicts).encode("utf-8")
    return gzip.compress(json_bytes)

def getDataOmfMessageHeaders():
    return {
        "MessageType": "Data",
        "Action": "Create",
        "MessageFormat": "Json",
        "omfversion": "1.2",
        "compression": "gzip",
    }

def copyStreamOmf(stream, source_sds_source: ADHClient, source_namespace_id, destination_sds_source: ADHClient, destination_namespace_id, start_time, end_time, prefix=''):
    # Create the new stream
    source_stream_id = stream.Id
    stream.Id = f'{prefix}{stream.Id}'  # optional
    stream.TypeId = f'{prefix}{stream.TypeId}'  # optional
    destination_sds_source.Streams.getOrCreateStream(
        namespace_id=destination_namespace_id, stream=stream)
    
    # Construct omf endpoint
    omf_endpoint = f'{destination_sds_source.uri}/api/v1/Tenants/{destination_sds_source.tenant}/Namespaces/{destination_namespace_id}/omf'

    # Copy the values from the current stream to the new stream, using omf message to send data to destination
    events_per_chunk = 10_000
    data = source_sds_source.Streams.getWindowValuesPaged(
        namespace_id=source_namespace_id, stream_id=source_stream_id, value_class=None,
        start=start_time, end=end_time, count=events_per_chunk)
    if len(data.Results) > 0:
        # Use base client to call omf endpoint
        response = destination_sds_source.baseClient.request(
            method="post", 
            url=omf_endpoint, 
            params=None, 
            data=prepareDataOmfMessageGzipContent(data, stream.Id), 
            headers=None, 
            additional_headers=getDataOmfMessageHeaders())
    
    while not data.end():
        data = source_sds_source.Streams.getWindowValuesPaged(
            namespace_id=source_namespace_id, stream_id=source_stream_id, value_class=None,
            start=start_time, end=end_time, count=events_per_chunk, continuation_token=data.ContinuationToken)
        if len(data.Results) > 0:
            # Use base client to call omf endpoint
            response = destination_sds_source.baseClient.request(
                method="post", 
                url=omf_endpoint, 
                params=None, 
                data=prepareDataOmfMessageGzipContent(data, stream.Id), 
                headers=None, 
                additional_headers=getDataOmfMessageHeaders()
            )
        

def copyAsset(asset, source_sds_source, source_namespace_id, destination_sds_source, destination_namespace_id, prefix=''):

    # Ensure type exists in new namespace
    if asset.AssetTypeId != None:
        destination_asset_type = source_sds_source.AssetTypes.getAssetTypeById(
            namespace_id=source_namespace_id, asset_type_id=asset.AssetTypeId)

        destination_asset_type.Id = f'{prefix}{asset.AssetTypeId}'  # optional
        if destination_asset_type.TypeReferences != None:
            for type_reference in destination_asset_type.TypeReferences:
                # optional
                type_reference.TypeId = f'{prefix}{type_reference.TypeId}'

        destination_sds_source.AssetTypes.createOrUpdateAssetType(
            namespace_id=destination_namespace_id, asset_type=destination_asset_type)
        asset.AssetTypeId = destination_asset_type.Id
    
    # Create the new asset

    asset.Id = f'{prefix}{asset.Id}'  # optional
    destination_sds_source.Assets.createOrUpdateAsset(
        namespace_id=destination_namespace_id, asset=asset)


def removeDuplicates(list):

    id_set = set()
    reduced_list = []
    for item in list:
        if item.Id not in id_set:
            reduced_list.append(item)
            id_set.add(item.Id)

    return reduced_list


def main(test=False):

    streams = []  # The list of streams to be sent
    assets = []  # The list of assets to be sent

    if test:
        prefix = appsettings.test_prefix
    else:
        prefix = appsettings.prefix

    try:
        # Step 1: Copy data view

        if appsettings.data_view_id != None:
            # Find the data view with the specified Id
            data_view = appsettings.source_sds_source.DataViews.getDataView(
                namespace_id=appsettings.source_namespace_id, data_view_id=appsettings.data_view_id)

            # Copy all assets or streams that are referenced by the data view
            for query in data_view.Queries:

                if query.Kind == DataItemResourceType.Stream:
                    new_streams = appsettings.source_sds_source.Streams.getStreams(
                        namespace_id=appsettings.source_namespace_id, query=query.Value, skip=0, count=appsettings.max_stream_count)
                    streams += new_streams

                elif query.Kind == DataItemResourceType.Assets:
                    new_assets = appsettings.source_sds_source.Assets.getAssets(
                        namespace_id=appsettings.source_namespace_id, query=query.Value, skip=0, count=appsettings.max_asset_count)
                    assets += new_assets

                else:
                    raise ValueError

            # Create the data view
            data_view.Id = f'{prefix}{data_view.Id}'  # optional
            appsettings.destination_sds_source.DataViews.putDataView(
                namespace_id=appsettings.destination_namespace_id, data_view=data_view)

        # Step 2: Retrieve streams referenced in assets

        if appsettings.asset_query != None:
            # Find all assets via a query (repeat script if multiple searches are necessary)
            assets += appsettings.source_sds_source.Assets.getAssets(
                namespace_id=appsettings.source_namespace_id, query=appsettings.asset_query, skip=0, count=appsettings.max_asset_count)

        # Remove duplicate assets
        assets = removeDuplicates(assets)

        # Copy over referenced streams
        for asset in assets:
            if str(asset.StreamReferences) != 'None':
                for stream_reference in asset.StreamReferences:
                    stream = appsettings.source_sds_source.Streams.getStream(
                        namespace_id=appsettings.source_namespace_id, stream_id=stream_reference.StreamId)
                    # optional
                    stream_reference.StreamId = f'{prefix}{stream_reference.StreamId}'
                    streams.append(stream)

        # Step 3: Copy streams
        
        if appsettings.stream_query != None:
            # Find all streams via a query (repeat script if multiple searches are necessary)
            streams += appsettings.source_sds_source.Streams.getStreams(
                namespace_id=appsettings.source_namespace_id, query=appsettings.stream_query, skip=0, count=appsettings.max_stream_count)

        # Remove duplicate streams
        streams = removeDuplicates(streams)

        # Get a list of all unique types to create
        types_id_set = set()

        for stream in streams:

            if stream.TypeId not in types_id_set:
                types_id_set.add(stream.TypeId)

        # Copy each unique type found
        with ThreadPoolExecutor() as pool:
            for type_id in types_id_set:
                pool.submit(copyTypeByTypeId, type_id, appsettings.source_sds_source, appsettings.source_namespace_id,
                            appsettings.destination_sds_source, appsettings.destination_namespace_id, prefix)

        # For each stream found create the new stream and copy the values
        # Set max workers to limit concurrency to a certain number streams to migrate at a time
        with ThreadPoolExecutor(max_workers=8) as pool:
            for stream in streams:
                pool.submit(copyStreamOmf, stream, appsettings.source_sds_source, appsettings.source_namespace_id,
                            appsettings.destination_sds_source, appsettings.destination_namespace_id, appsettings.start_time, appsettings.end_time, prefix)

        # Step 4: Copy assets

        with ThreadPoolExecutor() as pool:
            for asset in assets:
                pool.submit(copyAsset, asset, appsettings.source_sds_source, appsettings.source_namespace_id,
                            appsettings.destination_sds_source, appsettings.destination_namespace_id, prefix)

    except Exception as ex:
        print((f"Encountered Error: {ex}"))
        print
        traceback.print_exc()
        print


if __name__ == '__main__':

    main()
