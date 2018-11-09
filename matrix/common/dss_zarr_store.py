# -*- coding: utf-8 -*-
"""Class for working with expression matrices serialized in the DCP."""

from collections import MutableMapping
import hashlib
import os

import hca
import zarr
from hca import HCAConfig

from matrix.common.exceptions import MatrixException


class DSSZarrStore(MutableMapping):
    """
    Zarr compatible interface to expression matrices stored in a DCP analysis bundle.
    Parameters
    ----------
    dss_client : hca.dss.DSSClient
        Client used to access the DCP Data Storage System.
    bundle_uuid : str
        DCP uuid of the analysis bundle.
    bundle_version : str, optional
        DCP version of the analysis bundle. (default is None, which means use
        the latest version of the bundle)
    replica : str, optional
        Replica of the DCP from which to access data. (default is "aws")
    Attributes
    ----------
    bundle_uuid
    bundle_version
    Methods
    -------
    Provides the MutableMapping interface, as required by zarr. But bad news: the DCP isn't
    mutable, so mutations raise NotImplementedError with a helpful note.
    """

    # The DSS doesn't allow "directories", which really means it doesn't allow
    # file keys with a '/' in the name since none of the storage backends has a
    # directory concept anyway. So, we replace '/' with a different separator.
    _separator_char = "!"

    # There are possibly many files in the analysis bundle, but we only want to
    # read those that are associated with the expression matrix. We can
    # identify those via a substring present in the name
    _zarr_substring = ".zarr" + _separator_char

    def _transform_key(self, key):
        return key.replace("/", self._separator_char)

    def __init__(self, bundle_uuid, bundle_version=None, replica="aws"):
        """
        :param bundle_uuid: unique identifier for bundle in dss
        :param bundle_version: (optional) version tag for bundle in dss
        :param replica: (optional) "aws", "gcp", or "azure" to reflect dss cloud
        """
        dss_stage = os.getenv('DSS_STAGE', "integration")
        self._dss_client = self._get_dss_client(dss_stage)

        self._bundle_uuid = bundle_uuid
        self._bundle_version = bundle_version
        self._replica = replica

        # Read the bundle contents once since it's pretty slow. Just keep the
        # file info that we want
        bundle_contents = self._dss_client.get_bundle(
            uuid=self._bundle_uuid,
            version=self._bundle_version,
            replica=self._replica
        )

        self._bundle_contents = {}
        for dcp_file in bundle_contents["bundle"]["files"]:
            if self._zarr_substring in dcp_file["name"]:

                # Names look like this:
                # 586fdb4e-b533-40b1-b9b0-0d9211d0b198.zarr!cell_metadata_string_name!.zarray
                # we want this
                # cell_metadata_string_name!.zarray
                normalized_name = dcp_file["name"][
                    dcp_file["name"].index(self._zarr_substring) + len(self._zarr_substring):]

                self._bundle_contents[normalized_name] = {
                    "uuid": dcp_file["uuid"],
                    "version": dcp_file["version"],
                    "sha1": dcp_file["sha1"]
                }

        self._root = zarr.group(store=self)
        self._validate_zarr()

    @property
    def bundle_uuid(self):
        """str : DCP uuid of the analysis bundle."""
        return self._bundle_uuid

    @property
    def bundle_version(self):
        """str or None : DCP version of the analysis bundle. If None, use the
        latest version.
        """
        return self._bundle_version

    @property
    def expression(self):
        return self._root.expression

    @property
    def cell_id(self):
        return self._root.cell_id

    @property
    def cell_metadata(self):
        return self._root.cell_metadata

    @property
    def cell_metadata_name(self):
        return self._root.cell_metadata_name

    @property
    def gene_id(self):
        return self._root.cell_id

    @property
    def gene_metadata(self):
        return self._root.gene_metadata

    @property
    def gene_metadata_name(self):
        return self._root.gene_metadata_name

    def _get_dss_client(self, dss_instance):
        # Default DSS config is unreachable when a user defined config dir is supplied.
        # This workaround supplies an explicit DSS config to avoid reading the config dir.
        # TODO: Fix user set config dir issue in DSS
        dss_config = HCAConfig()
        dss_config['DSSClient'] = {}
        dss_config['DSSClient']['swagger_url'] = f"https://dss.{dss_instance}.data.humancellatlas.org/v1/swagger.json"

        client = hca.dss.DSSClient(config=dss_config)
        return client

    def _validate_zarr(self):
        expected_fields = ['expression', 'cell_id', 'cell_metadata_string', 'cell_metadata_numeric',
                           'cell_metadata_string_name', 'cell_metadata_numeric_name', 'gene_id']
        if any(field not in self._root for field in expected_fields):
            raise MatrixException(400, f"Unable to process bundle {self.bundle_uuid}.{self.bundle_version}. "
                                       f"Invalid or no expression data found.")

    def __setitem__(self, key, value):
        raise NotImplementedError("The HCA Data Storage System is read-only.")

    def __delitem__(self, key):
        raise NotImplementedError("The HCA Data Storage System is read-only.")

    def __getitem__(self, key):

        transformed_key = self._transform_key(key)

        dcp_file = self._bundle_contents[transformed_key]

        with self._dss_client.get_file.stream(uuid=dcp_file["uuid"],
                                              version=dcp_file["version"],
                                              replica=self._replica) as handle:
            raw_obj = handle.raw.read()

        sha1 = hashlib.sha1(raw_obj).hexdigest()
        expected_sha1 = dcp_file["sha1"]

        if sha1 != expected_sha1:
            raise RuntimeError(
                "Corrupted read from DCP: hashes do not match for {transformed_key}: "
                "got {sha1} but expected {expected_sha1}.".format(
                    transformed_key=transformed_key, sha1=sha1, expected_sha1=expected_sha1))

        return raw_obj

    def __contains__(self, key):

        transformed_key = self._transform_key(key)
        return transformed_key in self._bundle_contents

    def __eq__(self, other):
        return (
            isinstance(other, DSSZarrStore) and
            self._bundle_uuid == other._bundle_uuid and
            self._bundle_version == other._bundle_version
        )

    def keys(self):
        return (k.replace(self._separator_char, "/") for k in self._bundle_contents)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())
