"""Tests for STAC endpoints."""

import pystac
import pystac_client
import pytest
import requests
from pystac_client.item_search import FilterLike

from esgf_stac_tests.tests.conftest import PerEndpointSuite

# What filters shall we test?
CQL_FILTERS: list[FilterLike] = [
    {
        "op": "or",
        "args": [
            {
                "args": [{"property": "properties.cmip6:variable_id"}, "rsus"],
                "op": "=",
            },
            {
                "args": [{"property": "properties.cmip6:variable_id"}, "rsds"],
                "op": "=",
            },
        ],
    },
    # ---------------------------------------------------
    {
        "op": "in",
        "args": [{"property": "properties.cmip6:variable_id"}, ["rsus", "rsds"]],
    },
    # ---------------------------------------------------
    {
        "op": "and",
        "args": [
            {
                "args": [{"property": "properties.cmip6:variable_id"}, "tas"],
                "op": "=",
            },
            {
                "args": [{"property": "properties.cmip6:source_id"}, "MIROC6"],
                "op": "=",
            },
        ],
    },
    # ---------------------------------------------------
    {
        "args": [{"property": "properties.cmip6:member_id"}, "r2i1p1f1"],
        "op": "=",
    },
    # ---------------------------------------------------
    {
        "args": [{"property": "properties.cmip6:variant_label"}, "r2i1p1f1"],
        "op": "=",
    },
    # ---------------------------------------------------
]

# Which collections do we expect the API to find?
SUPPORTED_COLLECTIONS: list[str] = ["CMIP6"]

# Which time ranges do we check?
TIME_RANGES: list[str] = [("1850-01-01", "2020-01-01")]


class TestStacEndpoints(PerEndpointSuite):
    """Tests for all STAC endpoints.

    In addition to their individual parameters, these tests are automatically
    parameterized by endpoint_url by the parent class.

    The `--stac-endpoints` command line option or the `stac_endpoints` ini option can be
    used to specify a comma-separated list of endpoints to test against. The default
    endpoints are defined in conftest.py.
    """

    @pytest.mark.parametrize("search_filter", CQL_FILTERS)
    def test_searching_with_filters(self, endpoint_url: str, search_filter: FilterLike) -> None:
        """Verify that filtered searches return results."""
        client = pystac_client.Client.open(endpoint_url)
        page = next(iter(client.search(collections="CMIP6", filter=search_filter).pages_as_dicts()))
        assert page["numMatched"] > 0

    def test_assets_include_file_extention_attributes(self, endpoint_url: str) -> None:
        """Verify Item Assets include the file:size and file:checksum attributes from the `file` STAC extension."""
        client = pystac_client.Client.open(endpoint_url)

        search_pages = client.search(
            collections="CMIP6",
            filter={
                "op": "in",
                "args": [
                    {"property": "properties.cmip6:variable_id"},
                    ["rsus", "rsds"],
                ],
            },
        ).pages()
        first_page = next(search_pages)
        asset = first_page.items[0].get_assets(media_type="application/netcdf")["data0001"]

        assert "file:size" in asset.extra_fields
        assert "file:checksum" in asset.extra_fields

    @pytest.mark.timeout(60)
    def test_pagination(self, endpoint_url: str) -> None:
        """Verify that all results can be retrieved by paging through them."""
        client = pystac_client.Client.open(endpoint_url)

        search_pages = client.search(
            collections="CMIP6",
            filter={
                "op": "in",
                "args": [
                    {"property": "properties.cmip6:variable_id"},
                    ["rsus", "rsds"],
                ],
            },
        ).pages_as_dicts()
        first_page = next(search_pages)

        expected_pages = int(first_page["numMatched"] / first_page["numReturned"])
        actual_pages = sum(1 for _ in search_pages)

        assert actual_pages == expected_pages

    @pytest.mark.xfail(reason="CMIP6 STAC extension used is not public")
    def test_validate_catalog(self, endpoint_url: str) -> None:
        """Validate the STAC catalog for the endpoint against the STAC spec."""
        pystac_client.Client.open(endpoint_url).validate_all(max_items=1)

    @pytest.mark.xfail(reason="Temporary design decision")
    def test_endpoint_uses_published_cmip6_extension(self, endpoint_url: str) -> None:
        """
        Check that the endpoint is using the published STAC CMIP6 extension.

        Note
        ----
        This is more to help us understand when differences in test results could be
        because an endpoint is pointing to a different extension.
        """
        published_schema_url = "https://stac-extensions.github.io/cmip6/v2.0.0/schema.json"

        client = pystac_client.Client.open(endpoint_url)
        response = client.search(collections="CMIP6", max_items=1)
        extensions = response.item_collection_as_dict()["features"][0]["stac_extensions"]

        if published_schema_url in extensions:
            return  # All good, using the published extension

        published_schema = requests.get(published_schema_url).json()

        cmip6_extension = [url for url in extensions if "cmip6" in url]
        assert cmip6_extension, "No CMIP6 STAC extension found."
        assert len(cmip6_extension) == 1, f"Multiple possible cmip6 extensions found: {cmip6_extension}"
        cmip6_url = cmip6_extension[0]
        cmip6_schema = requests.get(cmip6_url).json()

        # Assertion on dicts will give a diff if they are not the same so we can see what changes were needed
        assert cmip6_schema == published_schema

    def test_cmip6_collection_geospatial_extent(self, endpoint_url: str) -> None:
        """Check for expected collections and print their descriptions.

        Note
        ----
        It may be that this is handled in STAC's validate_all().
        """
        client = pystac_client.Client.open(endpoint_url)

        cmip6_coll = client.get_collection("CMIP6")

        cmip6_coll_extent = cmip6_coll.extent.to_dict()

        assert cmip6_coll_extent
        assert "spatial" in cmip6_coll_extent
        assert "temporal" in cmip6_coll_extent
        assert "bbox" in cmip6_coll_extent["spatial"]
        assert "interval" in cmip6_coll_extent["temporal"]

    def test_collections(self, endpoint_url: str) -> None:
        """Check for expected collections."""
        client = pystac_client.Client.open(endpoint_url)
        assert set(SUPPORTED_COLLECTIONS).issubset(
            [coll.id for coll in client.get_collections()],
        )

    @pytest.mark.parametrize("time_filter_method", ["datetime", "query", "filter"])
    @pytest.mark.parametrize("time_range", TIME_RANGES)
    def test_cmip6_temporal_query(
        self,
        endpoint_url: str,
        time_range: tuple[str, str],
        time_filter_method: str,
    ) -> None:
        """Can we filter out records by a time filter of any sort.

        Note
        ----
        I cannot seem to make this work for either endpoint. It may be a problem
        with publishing?
        """
        time_start, time_end = time_range
        # various methods that time ranges can be 'queried'
        args = {
            "datetime": f"{time_start}/{time_end}",
            "query": [f"start_datetime>{time_start}", f"end_datetime<{time_end}"],
            "filter": {
                "op": "t_intersects",
                "args": [
                    {"property": "start_datetime"},
                    f"{time_start}/{time_end}",
                ],
            },
        }
        client = pystac_client.Client.open(endpoint_url)
        item_search = client.search(
            collections=["CMIP6"],
            max_items=1,
            **{time_filter_method: args[time_filter_method]},
        )
        next(iter(item_search.items()))

    def test_item_content(self, endpoint_url: str) -> None:
        """Check that we can harvest an asset url."""
        client = pystac_client.Client.open(endpoint_url)
        item_search = client.search(collections=["CMIP6"], max_items=1)
        item = next(iter(item_search.items()))
        assert isinstance(item, pystac.item.Item)
        nc_assets = [v.href for _, v in item.assets.items() if v.href.endswith(".nc")]
        assert len(nc_assets) > 0
        nc_file_url = nc_assets[0]
        assert nc_file_url

    def test_facet_counts(self, endpoint_url: str) -> None:
        """Can we get facet counts.

        Note
        ----
        I don't think that pystac does aggregations so we will use search and then
        hack the url. This tests is a placeholder and needs improved as the
        capability grows.
        """
        client = pystac_client.Client.open(endpoint_url)
        results = client.search(
            collections=["CMIP6"],
            filter={
                "args": [{"property": "properties.cmip6:activity_id"}, "VolMIP"],
                "op": "=",
            },
        )
        url = results.url_with_parameters()
        url = url.replace(
            "search?",
            "aggregate?aggregations=cmip6_source_id_frequency,cmip6_table_id_frequency&",
        )
        response = requests.get(url)
        response.raise_for_status()
        content = response.json()
        out = {agg["name"]: [b["key"] for b in agg["buckets"]] for agg in content["aggregations"]}
        assert "cmip6_source_id_frequency" in out
        assert "cmip6_table_id_frequency" in out
        assert len(out["cmip6_source_id_frequency"]) > 0
        assert len(out["cmip6_table_id_frequency"]) > 0
