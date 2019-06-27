# -*- coding: utf-8 -*-
"""Class for working with expression matrices serialized in the DCP."""

import glob
import os
from collections.abc import MutableMapping


class DCPZarrStore(MutableMapping):
    """
    Zarr compatible interface to expression matrices stored in a DCP analysis bundle.
    Parameters
    ----------
    bundle_dir: str
        Local directory where a DCP analysis bundle is stored.
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
    # identify those via a pattern in the filename
    _zarr_pattern = f"*.zarr{_separator_char}*"

    def __init__(self, bundle_dir: str):
        """
        :param bundle_dir: path to local dir containing analysis bundle
        """
        self.bundle_dir = bundle_dir
        self.cache = {}
        self.zarr_files = glob.glob(os.path.join(self.bundle_dir, DCPZarrStore._zarr_pattern))
        self.zarr_prefix = self.zarr_files[0].split(DCPZarrStore._separator_char, 1)[0]

    def _transform_key(self, key):
        return key.replace("/", self._separator_char)

    def __setitem__(self, key, value):
        raise NotImplementedError("DCPZarrStore is read-only.")

    def __delitem__(self, key):
        raise NotImplementedError("DCPZarrStore is read-only.")

    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]

        transformed_key = self._transform_key(key)
        path_to_file = os.path.join(self.bundle_dir,
                                    DCPZarrStore._separator_char.join([self.zarr_prefix, transformed_key]))
        with open(path_to_file, 'rb') as fh:
            contents = fh.read()
            self.cache[key] = contents

        return contents

    def __contains__(self, key):
        transformed_key = self._transform_key(key)
        return transformed_key in self.keys()

    def __eq__(self, other):
        return (
            isinstance(other, DCPZarrStore) and
            self.zarr_files == other.zarr_files
        )

    def keys(self):
        return (k.split(DCPZarrStore._separator_char, 1)[1] for k in self.zarr_files)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())
