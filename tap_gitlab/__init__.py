#!/usr/bin/env python3

import datetime
import json
import sys
import os
from pprint import pprint

import pendulum
import requests
import singer
from singer import Transformer, utils, Schema, Catalog, CatalogEntry, write_bookmark

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp

PER_PAGE = 100
REQUIRED_CONFIG_KEYS = ["private_token", "start_date"]
STATE = {}
LOGGER = singer.get_logger()
SESSION = requests.Session()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# def load_schema(entity):
#     return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


SCHEMAS = {
    'projects': {
        'url': '/projects/{}',
        'replication_key': 'last_activity_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{}/repository/branches',
        'replication_method': 'FULL',
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{}/repository/commits',
        'replication_key': 'created_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{}/issues',
        'replication_key': 'updated_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{}/users',
        'replication_method': 'FULL',
        'key_properties': ['id'],
    },
    'groups': {
        'url': '/groups/{}',
        'key_properties': ['id'],
    },
    'deployments': {
        'url': '/projects/{}/deployments',
        'replication_key': 'updated_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['id'],
    },
    'pipelines': {
        'url': '/projects/{}/pipelines',
        'replication_key': 'updated_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['id'],
    },
    'releases': {
        'url': '/projects/{}/releases',
        'replication_key': 'released_at',
        'replication_method': 'INCREMENTAL',
        'key_properties': ['tag_name'],
    },
}


def get_value(stream_id, key, default=None):
    if stream_id not in SCHEMAS:
        return default
    if key not in SCHEMAS[stream_id]:
        return default
    return SCHEMAS[stream_id][key]


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=get_value(stream_id, 'key_properties', list()),
                metadata=stream_metadata,
                replication_key=get_value(stream_id, 'replication_key'),
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=get_value(stream_id, 'replication_method'),
            )
        )
    return Catalog(streams)


def get_date_filter_param(entity, fallback_date):
    date_filtering = {
        "branches": '',
        "commits": "since",
        "deployments": "updated_after",
        "groups": '',
        "issues": "created_after",
        "milestones": '',
        "pipelines": "updated_after",
        "projects": "last_activity_after",
        "users": '',
        "releases": '',
    }

    dt = STATE.get(entity) or fallback_date

    return f'?{date_filtering.get(entity)}={dt}'


def get_url(config, entity, id, extract_from=None):
    if not isinstance(id, int):
        id = id.replace("/", "%2F")

    return config['api_url'] + SCHEMAS[entity]['url'].format(id) + get_date_filter_param(entity, extract_from)


def get_start(config, entity):
    if entity not in STATE:
        STATE[entity] = config['start_date']

    return STATE[entity]


def persist_state(filename, state):
    with open(filename, 'w+') as f:
        json.dump(state, f, indent=2)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(config, url, params=None):
    params = params or {}
    params['private_token'] = config['private_token']

    headers = {}
    if 'user_agent' in config:
        headers['User-Agent'] = config['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        LOGGER.critical(
            "Error making request to GitLab API: GET {} [{} - {}]".format(
                req.url, resp.status_code, resp.content))
        sys.exit(1)

    return resp


def gen_request(config, url):
    params = {'page': 1}
    resp = request(config, url, params)
    last_page = int(resp.headers.get('X-Total-Pages', 1))

    for row in resp.json():
        yield row

    for page in range(2, last_page + 1):
        params['page'] = page
        resp = request(config, url, params)
        for row in resp.json():
            yield row


def format_timestamp(data, typ, schema):
    result = data
    if typ == 'string' and schema.get('format') == 'date-time':
        rfc3339_ts = rfc3339_to_timestamp(data)
        utc_dt = datetime.datetime.utcfromtimestamp(rfc3339_ts).replace(tzinfo=pytz.UTC)
        result = utils.strftime(utc_dt)

    return result


def flatten_id(item, target):
    if target in item and item[target] is not None:
        item[target + '_id'] = item.pop(target, {}).pop('id', None)
    else:
        item[target + '_id'] = None


def sync_branches(config, stream, project):
    if stream.is_selected():
        url = get_url(config, "branches", project['id'])
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                row['project_id'] = project['id']
                flatten_id(row, "commit")
                row['branch_id'] = '{}_{}_{}'.format(row['project_id'], row['name'], row['commit_id'])
                row['extracted_at'] = utils.strftime(utils.now())
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                singer.write_record("branches", transformed_row, time_extracted=utils.now())


def sync_commits(config, stream, project):
    if stream.is_selected():
        extract_from = STATE.get(f'project_{project["id"]}') or config['start_date']
        url = get_url(config, "commits", project['id'], extract_from)
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                row['project_id'] = project["id"]
                row['extracted_at'] = utils.strftime(utils.now())
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                singer.write_record("commits", transformed_row, time_extracted=utils.now())


def sync_issues(config, stream, project):
    if stream.is_selected():
        extract_from = STATE.get(f'project_{project["id"]}') or config['start_date']
        url = get_url(config, "issues", project['id'], extract_from)
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                flatten_id(row, "author")
                flatten_id(row, "assignee")
                flatten_id(row, "milestone")
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                singer.write_record("issues", transformed_row, time_extracted=utils.now())


def sync_users(config, stream, project):
    if stream.is_selected():
        url = get_url(config, "users", project['id'])
        project["users"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                project["users"].append(row["id"])
                singer.write_record("users", transformed_row, time_extracted=utils.now())


def sync_deployments(config, stream, project):
    if stream.is_selected():
        extract_from = STATE.get(f'project_{project["id"]}') or config['start_date']
        url = get_url(config, "deployments", project['id'], extract_from)
        project["deployments"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                project["deployments"].append(row["id"])
                singer.write_record("deployments", transformed_row, time_extracted=utils.now())


def sync_pipelines(config, stream, project):
    if stream.is_selected():
        extract_from = STATE.get(f'project_{project["id"]}') or config['start_date']
        url = get_url(config, "pipelines", project['id'], extract_from)
        project["pipelines"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                project["pipelines"].append(row["id"])
                singer.write_record("pipelines", transformed_row, time_extracted=utils.now())


def sync_releases(config, stream, project):
    if stream.is_selected():
        extract_from = STATE.get(f'project_{project["id"]}') or config['start_date']
        url = get_url(config, "releases", project['id'])
        project["releases"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            for row in gen_request(config, url):
                transformed_row = transformer.transform(row, stream.schema.to_dict())
                project["releases"].append(row["tag_name"])
                if row["released_at"] >= extract_from:
                    singer.write_record("releases", transformed_row, time_extracted=utils.now())


def sync_group(config, catalog, gid, state_path):
    url = config['api_url'] + SCHEMAS["groups"]['url'].format(gid)

    data = request(config, url).json()
    pids = []
    stream = catalog.get_stream('groups')

    with Transformer(pre_hook=format_timestamp) as transformer:
        group = transformer.transform(data, stream.schema.to_dict())

    for project in group['projects']:
        if project['id']:
            pids.append(project['id'])

    for pid in pids:
        sync_project(config, catalog, pid)
        persist_state(state_path, STATE)

    singer.write_record("groups", group, time_extracted=utils.now())


def sync_project(config, catalog, pid):
    url = get_url(config, "projects", pid)
    data = request(config, url).json()
    stream = catalog.get_stream('projects')

    with Transformer(pre_hook=format_timestamp) as transformer:
        flatten_id(data, "owner")
        project = transformer.transform(data, stream.schema.to_dict())

    state_key = "project_{}".format(project["id"])

    #pylint: disable=maybe-no-member
    last_activity_at = project.get('last_activity_at', project.get('created_at'))
    if not last_activity_at:
        raise Exception(
            #pylint: disable=line-too-long
            "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
            .format(project['id']))

    current_bookmark = singer.get_bookmark(STATE, state_key, 'last_activity_at', config['start_date'])
    new_bookmark = pendulum.now().to_iso8601_string()
    LOGGER.info(f'Syncing project {pid} from {current_bookmark} to now. New bookmark will be {new_bookmark}.')

    if project['last_activity_at'] >= current_bookmark:
        sync_branches(config, catalog.get_stream('branches'), project)
        sync_commits(config, catalog.get_stream('commits'), project)
        sync_issues(config, catalog.get_stream('issues'), project)
        sync_users(config, catalog.get_stream('users'), project)
        sync_deployments(config, catalog.get_stream('deployments'), project)
        sync_pipelines(config, catalog.get_stream('pipelines'), project)
        sync_releases(config, catalog.get_stream('releases'), project)

        singer.write_record("projects", project, time_extracted=utils.now())
        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)
        write_bookmark(STATE, state_key, 'last_activity_at', new_bookmark)


def sync(args, catalog):
    LOGGER.info("Starting sync")

    config = args.config
    STATE.update(args.state)

    gids = list(filter(None, config['groups'].split(' ')))
    pids = list(filter(None, config['projects'].split(' ')))

    for stream in catalog.get_selected_streams(STATE):
        singer.write_schema(stream.stream, stream.schema.to_dict(), SCHEMAS[stream.stream]['key_properties'])

    for gid in gids:
        sync_group(config, catalog, gid, args.state_path)

    if not gids:
        # When not syncing groups
        for pid in pids:
            sync_project(config, catalog, pid)
            persist_state(args.state_path, STATE)

    LOGGER.info("Sync complete")


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args, catalog)


if __name__ == "__main__":
    main()
