"""Flask app that implements a feature of the HCA matrix service."""
import json
import multiprocessing
import os
import sqlite3
import tempfile
import uuid

import boto3
import hca
import loompy

from flask import Flask, g
from flask_restful import Resource, Api, reqparse, abort

BUCKET_NAME = "hca-matrix-service"

app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'app.db')
))

api = Api(app)


def connect_db():
    """Connect to the database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    """Return a reference to the database."""
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


class CreateQuery(Resource):
    """API endpoint to create a new matrix query."""

    def post(self):
        """Request a matrix be created. The "es_query" parameter is going to
        get passed directly to the DSSClient.

        This is perhaps the only query that works:
        '{"query": {"term": {"files.analysis_json.outputs.file_core.file_format": "loom"}}}'
        """

        # Get the query from the post body
        post_query_parser = reqparse.RequestParser()
        post_query_parser.add_argument("es_query")
        args = post_query_parser.parse_args()

        # Maybe someone has made this query before. If so, just return that
        db = get_db()
        cur = db.execute('select * from queries where query_text = ?',
                         (args.es_query,))
        result = cur.fetchone()
        if result:
            return {"query_id": result['query_id']}

        # Okay no, so we have to make a new query
        query_id = str(uuid.uuid4())
        es_query = json.loads(args.es_query)

        db.execute('insert into queries (query_id, query_text, status) values (?, ?, ?)',
                   (query_id, args.es_query, "Submitted"))
        db.commit()

        # Launch the matrix creation job in the background and return the query id
        proc = multiprocessing.Process(
            target=run_dss_query,
            args=(es_query, query_id))
        proc.daemon = True
        proc.start()

        return {"query_id": query_id}


class RetrieveQuery(Resource):
    """API endpoint to get the status and URL of a previous matrix query request."""

    def get(self, query_id):
        """Get information about a particular query id."""

        db = get_db()
        cur = db.execute('select * from queries where query_id = ?',
                         (query_id,))
        result = cur.fetchone()

        if not result:
            abort(404, message="Query Id {} not found.".format(query_id))

        response = {
            "query_id": query_id,
            "status": result["status"],
            "url": ""
        }

        if result["status"] == "Complete":
            s3 = boto3.client('s3')
            url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    "Bucket": result["bucket"],
                    "Key": result["key"]
                }
            )
            response["url"] = url

        return response


api.add_resource(CreateQuery, '/queries')
api.add_resource(RetrieveQuery, '/queries/<query_id>')


def run_dss_query(query_dict, query_id):
    """Create a matrix based on the query and update the db when finished."""

    print("Running dss query with", query_dict, query_id)
    client = hca.dss.DSSClient()

    # Gather up uuids of all the matrix files we're going to merge
    files_to_merge = []
    for result in client.post_search.iterate(es_query=query_dict, replica="aws"):
        bundle_uuid = result["bundle_fqid"][:36] #strip off the version
        files = client.get_bundle(
            uuid=bundle_uuid, replica="aws")["bundle"]["files"]

        for file_ in files:
            if file_["name"] != "analysis.json":
                files_to_merge.append((file_["name"], file_["uuid"]))
                print("Adding file to merge", file_["name"])

    # Write all the matrix files into a temporary directory
    temp_dir = tempfile.mkdtemp()
    print("temp dir", temp_dir)

    local_mtx_paths = []
    for file_ in files_to_merge:
        path = os.path.join(temp_dir, file_[0])
        with open(path, "wb") as mtx:
            print("Writing to", path)
            mtx.write(client.get_file(uuid=file_[1], replica="aws"))
        local_mtx_paths.append(path)

    # In this case, use a loom function to merge all the files together
    out_file = os.path.join(temp_dir, query_id + ".loom")
    print("Combining to", out_file)
    loompy.combine(local_mtx_paths, out_file)
    print("Done combining")

    # Upload the merged file to an S3 bucket and update the db with that
    # information
    s3 = boto3.resource("s3")
    key = query_id + '.loom'
    with open(out_file, 'rb') as merged_matrix:
        s3.Bucket(BUCKET_NAME).put_object(Key=key, Body=merged_matrix)

    db = get_db()
    db.execute('update queries set status="Complete", bucket=?, key=? where query_id = ?',
               (BUCKET_NAME, key, query_id))
    db.commit()


if __name__ == '__main__':
    app.run(debug=True)
