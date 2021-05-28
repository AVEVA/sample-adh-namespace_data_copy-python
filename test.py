import unittest
import configparser

from ocs_sample_library_preview import DataItemResourceType, SdsResultPage, OCSClient
from concurrent.futures import ThreadPoolExecutor
from program import main

from config import test_prefix, test_prefix, max_stream_count, max_asset_count, destination_sds_source, destination_namespace_id


class SDSPythonSampleTests(unittest.TestCase):
    @classmethod
    def test_main(cls):
        main(test=True)

        # Step 5: Cleanup
        cleanup()


def cleanup():

    # Settings
    stream_query = f'"{test_prefix}SLTCSensorUnit1"'
    asset_query = f'Name:"SLTC Sensor1" AND AssetTypeId:"{test_prefix}"*'
    data_view_id = f'{test_prefix}SLTC Sensor Unit'

    # Find the data view with the specified id and delete it
    try:
        destination_sds_source.DataViews.deleteDataView(
            namespace_id=destination_namespace_id, data_view_id=data_view_id)
    except Exception as ex:
        print((f"Encountered Error: {ex}"))
        print

    # Find all assets that have the test prefix and delete them
    assets = destination_sds_source.Assets.getAssets(
        namespace_id=destination_namespace_id, query=asset_query, skip=0, count=max_asset_count)
    asset_type_ids = set()
    with ThreadPoolExecutor() as pool:
        for asset in assets:
            asset_type_ids.add(asset.AssetTypeId)
            pool.submit(destination_sds_source.Assets.deleteAsset,
                        namespace_id=destination_namespace_id, asset_id=asset.Id)

    # Delete all asset types used by created assets
    for asset_type_id in asset_type_ids:
        # Check that the type id is not still in use
        assets = destination_sds_source.Assets.getAssets(
            namespace_id=destination_namespace_id, query=f'AssetTypeId:"{asset_type_id}"')

        if len(assets) > 0:
            print(
                f'Asset type use {asset_type_id} still in use! It will be skipped.')
        else:
            # Delete type
            destination_sds_source.Assets.deleteAssetType(
                namespace_id=destination_namespace_id, asset_type_id=asset_type_id)

    # Find all streams that have the test prefix and delete them
    streams = destination_sds_source.Streams.getStreams(
        namespace_id=destination_namespace_id, query=stream_query, skip=0, count=max_stream_count)
    type_ids = set()
    with ThreadPoolExecutor() as pool:
        for stream in streams:
            type_ids.add(stream.TypeId)
            pool.submit(destination_sds_source.Streams.deleteStream,
                        namespace_id=destination_namespace_id, stream_id=stream.Id)

    # Find all types associated with the created streams and delete them
    for type_id in type_ids:
        try:
            destination_sds_source.Types.deleteType(
                namespace_id=destination_namespace_id, type_id=type_id)
        except Exception as ex:
            print((f"Encountered Error: {ex}"))
            print


if __name__ == '__main__':
    unittest.main()
