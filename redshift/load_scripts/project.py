"""
CREATE TABLE project (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    proj_short_name       VARCHAR(100) NOT NULL,
    proj_title            VARCHAR(150) NOT NULL
) DISTSTYLE ALL;

CREATE TABLE publication (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    pub_title             VARCHAR(150) NOT NULL,
    pub_doi               VARCHAR(40)
) DISTSTYLE ALL;

CREATE TABLE contributor (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    cont_name             VARCHAR(150) NOT NULL,
    cont_institution      VARCHAR(150)
) DISTSTYLE ALL;
"""

import json
import pathlib

def parse_project_json(project_json_path):
    project_dict = json.load(open(project_json_path))
    key = project_dict["provenance"]["document_id"]
    short_name = project_dict["project_core"]["project_short_name"]
    if "_test_" in short_name:
        return None
    title = project_dict["project_core"]["project_title"]

    contributors = [
        (c["contact_name"], c.get("institution")) for c in project_dict.get("contributors", [])]

    publications = [
        (p.get("publication_title"), p.get("doi")) for p in project_dict.get("publications", [])]

    return {
        "key": key,
        "short_name": short_name,
        "title": title,
        "publications": publications,
        "contributors": contributors
    }

def main():
    p = pathlib.Path(".")

    project_infos = []
    for project_json_path in p.glob("**/project_*.json"):
        project_info = parse_project_json(project_json_path)
        if project_info:
            project_infos.append(project_info)


    project_data = set()
    for project_info in project_infos:
        project_data.add(
            '|'.join([project_info['key'], project_info['short_name'], project_info['title']]))
    with open("project.data", "w") as project_data_file:
        for project_line in project_data:
            project_data_file.write(project_line + '\n')

    contributor_data = set()
    for project_info in project_infos:
        for contributor in project_info.get("contributors", []):
            contributor_data.add(
                '|'.join([project_info["key"], contributor[0], contributor[1]]))
    with open("contributor.data", "w") as contributor_data_file:
        for contributor_line in contributor_data:
            contributor_data_file.write(contributor_line + '\n')

    publication_data = set()
    for project_info in project_infos:
        for publication in project_info.get("publications", []):
            publication_data.add(
                '|'.join([project_info["key"], publication[0], publication[1]]))
    with open("publication.data", "w") as publication_data_file:
        for publication_line in publication_data:
            publication_data_file.write(publication_line + '\n')

main()
