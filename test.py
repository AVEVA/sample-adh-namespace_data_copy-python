import unittest
import configparser

from ocs_sample_library_preview import DataItemResourceType, SdsResultPage, OCSClient
from concurrent.futures import ThreadPoolExecutor
from program import main


class SDSPythonSampleTests(unittest.TestCase):
    @classmethod
    def test_main(cls):
        main(test=True)

        # Step 5: Cleanup
        cleanup()


def cleanup():

    # Settings
    test_prefix = 'SAMPLE_TEST:'  # prefix for testing
    stream_query = f'"{test_prefix}SLTCSensorUnit1"'
    asset_query = f'Name:"SLTC Sensor1" AND AssetTypeId:"{test_prefix}"*'
    data_view_id = f'{test_prefix}SLTC Sensor Unit'
    max_stream_count = 150  # The maximum number of streams to retrieve
    max_asset_count = 150  # The maximum number of assets to retrieve
    request_timeout = 30  # The request timeout limit

    # Read configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    destination_sds_source = OCSClient(config.get('DestinationConfiguration', 'ApiVersion'),
                                       config.get(
                                           'DestinationConfiguration', 'TenantId'),
                                       config.get(
                                           'DestinationConfiguration', 'Resource'),
                                       config.get(
                                           'DestinationConfiguration', 'ClientId'),
                                       config.get('DestinationConfiguration', 'ClientSecret'))
    destination_sds_source.request_timeout = request_timeout

    destination_namespace_id = config.get(
        'DestinationConfiguration', 'NamespaceId')

    # Find all data views that have the test prefix and delete them
    data_views = destination_sds_source.DataViews.getDataViews(
        namespace_id=destination_namespace_id)
    for data_view in data_views:
        if data_view.Id == data_view_id:
            destination_sds_source.DataViews.deleteDataView(
                namespace_id=destination_namespace_id, data_view_id=data_view.Id)

    # Find all assets that have the test prefix and delete them
    assets = destination_sds_source.Assets.getAssets(
        namespace_id=destination_namespace_id, query=asset_query, skip=0, count=max_asset_count)
    asset_type_ids = set()
    with ThreadPoolExecutor() as pool:
        for asset in assets:
            asset_type_ids.add(asset.AssetTypeId)
            pool.submit(destination_sds_source.Assets.deleteAsset,
                        namespace_id=destination_namespace_id, asset_id=asset.Id)

    # Find all asset types that have the test prefix and delete them
    for asset_type_id in asset_type_ids:
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
        destination_sds_source.Types.deleteType(
            namespace_id=destination_namespace_id, type_id=type_id)


if __name__ == '__main__':
    unittest.main()
