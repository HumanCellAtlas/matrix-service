# -*- coding: utf-8 -*-
"""Classes for working with expression matrices serialized in the DCP."""

from collections import MutableMapping
import hashlib


class HCAStore(MutableMapping):
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
    # identify those via a prefix in the file name.
    _zarr_file_prefix = "expression_matrix"

    def _transform_key(self, key):
        return key.replace("/", self._separator_char)

    def __init__(self, dss_client, bundle_uuid, bundle_version=None, replica="aws"):

        self._dss_client = dss_client
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
            if dcp_file["name"].startswith(self._zarr_file_prefix):
                normalized_name = dcp_file["name"][len(self._zarr_file_prefix) + len(self._separator_char):]
                self._bundle_contents[normalized_name] = {
                    "uuid": dcp_file["uuid"],
                    "version": dcp_file["version"],
                    "sha1": dcp_file["sha1"]
                }

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
            isinstance(other, HCAStore) and
            self._bundle_uuid == other._bundle_uuid and
            self._bundle_version == other._bundle_version
        )

    def keys(self):
        return (k.replace(self._separator_char, "/") for k in self._bundle_contents)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())
