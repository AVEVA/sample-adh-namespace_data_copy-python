from ocs_sample_library_preview import DataItemResourceType, SdsResultPage, OCSClient
from concurrent.futures import ThreadPoolExecutor
import threading
import traceback
import configparser
import json

from config import stream_query, asset_query, data_view_id, test_prefix, test_prefix, \
    max_stream_count, max_asset_count, start_time, end_time, source_sds_source, \
    destination_sds_source, source_namespace_id, destination_namespace_id


def copyStream(stream, source_sds_source, source_namespace_id, destination_sds_source, destination_namespace_id, start_time, end_time, prefix=''):

    # Ensure type exists in new namespace
    destination_type = source_sds_source.Types.getType(
        namespace_id=source_namespace_id, type_id=stream.TypeId)
    destination_type.Id = f'{prefix}{stream.TypeId}'  # optional
    destination_sds_source.Types.getOrCreateType(
        namespace_id=destination_namespace_id, type=destination_type)

    # Create the new stream
    source_stream_id = stream.Id
    stream.Id = f'{prefix}{stream.Id}'  # optional
    stream.TypeId = destination_type.Id
    destination_sds_source.Streams.getOrCreateStream(
        namespace_id=destination_namespace_id, stream=stream)

    # Copy the values from the current stream to the new stream
    data = source_sds_source.Streams.getWindowValuesPaged(
        namespace_id=source_namespace_id, stream_id=source_stream_id, value_class=None,
        start=start_time, end=end_time, count=250000)
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


def copyAsset(asset, source_sds_source, source_namespace_id, destination_sds_source, destination_namespace_id, prefix=''):

    # Ensure type exists in new namespace
    if asset.AssetTypeId != None:
        destination_asset_type = source_sds_source.Assets.getAssetTypeById(
            namespace_id=source_namespace_id, asset_type_id=asset.AssetTypeId)

        destination_asset_type.Id = f'{prefix}{asset.AssetTypeId}'  # optional

        for type_reference in destination_asset_type.TypeReferences:
            # optional
            type_reference.TypeId = f'{prefix}{type_reference.TypeId}'

        destination_sds_source.Assets.createOrUpdateAssetType(
            namespace_id=destination_namespace_id, asset_type=destination_asset_type)
        asset.AssetTypeId = destination_asset_type.Id

    # Create the new asset
    asset.Id = f'{prefix}{asset.Id}'  # optional
    destination_sds_source.Assets.createOrUpdateAsset(
        namespace_id=destination_namespace_id, asset=asset)


def main(test=False):
    global streams, assets

    streams = []  # The list of streams to be sent
    assets = []  # The list of assets to be sent

    if test:
        prefix = test_prefix

    try:
        # Step 1: Copy data view

        if data_view_id != None:
            # Find the data view with the specified Id
            data_view = source_sds_source.DataViews.getDataView(
                namespace_id=source_namespace_id, data_view_id=data_view_id)

            # Copy all assets or streams that are referenced by the data view
            for query in data_view.Queries:

                if query.Kind == DataItemResourceType.Stream:
                    new_streams = source_sds_source.Streams.getStreams(
                        namespace_id=source_namespace_id, query=query.Value, skip=0, count=max_stream_count)
                    streams = streams + new_streams

                elif query.Kind == DataItemResourceType.Assets:
                    new_assets = source_sds_source.Assets.getAssets(
                        namespace_id=source_namespace_id, query=query.Value, skip=0, count=max_asset_count)
                    assets = assets + new_assets

                else:
                    raise ValueError

            # Create the data view
            data_view.Id = f'{prefix}{data_view.Id}'  # optional
            destination_sds_source.DataViews.putDataView(
                namespace_id=destination_namespace_id, data_view=data_view)

        # Step 2: Retrieve streams referenced in assets

        if asset_query != None:
            # Find all assets via a query (repeat script if multiple searches are necessary)
            assets = assets + source_sds_source.Assets.getAssets(
                namespace_id=source_namespace_id, query=asset_query, skip=0, count=max_asset_count)

        # Remove duplicate assets
        asset_id_set = set()
        reduced_assets = []
        for asset in assets:
            if asset.Id not in asset_id_set:
                reduced_assets.append(asset)
                asset_id_set.add(asset.Id)

        # Copy over referenced streams
        for asset in assets:
            for stream_reference in asset.StreamReferences:
                stream = source_sds_source.Streams.getStream(
                    namespace_id=source_namespace_id, stream_id=stream_reference.StreamId)
                # optional
                stream_reference.StreamId = f'{prefix}{stream_reference.StreamId}'
                streams.append(stream)

        # Step 3: Copy streams

        if stream_query != None:
            # Find all streams via a query (repeat script if multiple searches are necessary)
            streams = streams + source_sds_source.Streams.getStreams(
                namespace_id=source_namespace_id, query=stream_query, skip=0, count=max_stream_count)

        # Remove duplicate streams
        stream_id_set = set()
        reduced_streams = []
        for stream in streams:
            if stream.Id not in stream_id_set:
                reduced_streams.append(stream)
                stream_id_set.add(stream.Id)

        # For each stream found, copy over the type, create the new stream, and copy the values
        with ThreadPoolExecutor() as pool:
            for stream in reduced_streams:
                pool.submit(copyStream, stream, source_sds_source, source_namespace_id,
                            destination_sds_source, destination_namespace_id, start_time, end_time, prefix)

        # Step 4: Copy assets

        with ThreadPoolExecutor() as pool:
            for asset in reduced_assets:
                pool.submit(copyAsset, asset, source_sds_source, source_namespace_id,
                            destination_sds_source, destination_namespace_id, prefix)

    except Exception as ex:
        print((f"Encountered Error: {ex}"))
        print
        traceback.print_exc()
        print


if __name__ == '__main__':
    main()
