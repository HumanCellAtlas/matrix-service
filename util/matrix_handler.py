from abc import ABC, abstractmethod

import hca


class MatrixHandler(ABC):
    """
    A generic matrix handler for matrices concatenation
    """

    matrix_extension = None

    @classmethod
    def get_matrices_from_bundles(cls, uuids):
        """
        Filter for the matrix files within bundles

        :param uuids: A list of bundle uuids
        :return: A list of matrix files uuids
        """
        client = hca.dss.DSSClient()

        matrices_uuids = []

        # Iterate uuids to query DSS for bundle manifest of each
        for uuid in uuids:
            bundle_manifest = client.get_bundle(replica='aws', uuid=uuid)

            # Gather up uuids of all the matrix files we are going to merge
            for file in bundle_manifest['bundle']['files']:
                if file['name'].endswith(cls.matrix_extension):
                    matrices_uuids.append(file['uuid'])

        return matrices_uuids

    @classmethod
    @abstractmethod
    def concat(cls, uuids):
        """
        Concatenate a list of matrices, and save into a new file

        :param uuids: A list of matrix uuids
        :return: New matrix file's uuid
        """


class LoomMatrixHandler(MatrixHandler):
    """
    Matrix handler for .loom file format
    """

    matrix_extension = '.loom'

    @classmethod
    def concat(cls, uuids):
        pass


if __name__ == '__main__':
    bundle_uuids = (
        '4b1cf36d-7a99-4826-ac9c-6caf16facc70',
        'fa42b3f2-39c0-43a7-8c35-f8b1ef20725b',
        '6e0df416-0c74-4ccb-bd0e-546e0e751ce3',
        '6354d814-efd4-4084-9dd5-ba229a2f128c',
    )
    matrix_uuids = LoomMatrixHandler.get_matrices_from_bundles(bundle_uuids)
    print(str(matrix_uuids))
