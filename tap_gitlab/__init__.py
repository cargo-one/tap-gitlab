#!/usr/bin/env python3

import datetime
import sys
import os
import requests
import singer
from singer import Transformer, utils, strftime

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp

PER_PAGE = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v3",
    'private_token': None,
    'start_date': None,
    'groups': ''
}
STATE = {}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

RESOURCES = {
    'projects': {
        'url': '/projects/{}',
        'schema': load_schema('projects'),
        'key_properties': ['id'],
    },
    'branches': {
        'url': '/projects/{}/repository/branches',
        'schema': load_schema('branches'),
        'key_properties': ['project_id', 'name'],
    },
    'commits': {
        'url': '/projects/{}/repository/commits',
        'schema': load_schema('commits'),
        'key_properties': ['id'],
    },
    'issues': {
        'url': '/projects/{}/issues',
        'schema': load_schema('issues'),
        'key_properties': ['id'],
    },
    'project_milestones': {
        'url': '/projects/{}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
    },
    'group_milestones': {
        'url': '/groups/{}/milestones',
        'schema': load_schema('milestones'),
        'key_properties': ['id'],
    },
    'users': {
        'url': '/projects/{}/users',
        'schema': load_schema('users'),
        'key_properties': ['id'],
    },
    'groups': {
        'url': '/groups/{}',
        'schema': load_schema('groups'),
        'key_properties': ['id'],
    },
    'deployments': {
        'url': '/projects/{}/deployments',
        'schema': load_schema('deployments'),
        'key_properties': ['id'],
    },
    'pipelines': {
        'url': '/projects/{}/pipelines',
        'schema': load_schema('pipelines'),
        'key_properties': ['id'],
    },
    'releases': {
        'url': '/projects/{}/releases',
        'schema': load_schema('releases'),
        'key_properties': ['tag_name'],
    },
}


LOGGER = singer.get_logger()
SESSION = requests.Session()


def get_date_filter_param(entity, state_key):
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

    return f'?{date_filtering.get(entity)}={STATE.get(state_key)}'


def get_url(entity, id):
    state_key = "project_{}".format(id)
    if not isinstance(id, int):
        id = id.replace("/", "%2F")

    return CONFIG['api_url'] + RESOURCES[entity]['url'].format(id) + get_date_filter_param(entity, state_key)


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, params=None):
    params = params or {}
    params['private_token'] = CONFIG['private_token']

    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        raise Exception("Error making request to GitLab API: GET {} [{} - {}]".format(
                req.url, resp.status_code, resp.content))

    return resp


def gen_request(url):
    params = {'page': 1}
    resp = request(url, params)
    last_page = int(resp.headers.get('X-Total-Pages', 1))

    for row in resp.json():
        yield row

    for page in range(2, last_page + 1):
        params['page'] = page
        resp = request(url, params)
        for row in resp.json():
            yield row


def alt_gen_request(url):
    # Some endpoints use x-next-page instead of x-total-pages
    params = {'page': 1}
    resp = request(url, params)
    next_page = resp.headers.get('x-next-page', False)

    for row in resp.json():
        yield row

    while next_page:
        params['page'] = next_page
        resp = request(url, params)
        next_page = resp.headers.get('x-next-page', False)
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


def add_extraction_date(row, key='__extracted_at'):
    row[key] = strftime(utils.now())


def is_entity_in_state(entity):
    if STATE.get(entity):
        return True
    return False


def sync_branches(project):
    if is_entity_in_state('branches'):
        url = get_url("branches", project['id'])
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    row['project_id'] = project['id']
                    flatten_id(row, "commit")
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["branches"]["schema"])
                    singer.write_record("branches", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')


def sync_commits(project):
    if is_entity_in_state('commits'):
        url = get_url("commits", project['id'])
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in alt_gen_request(url):
                    row['project_id'] = project["id"]
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["commits"]["schema"])
                    singer.write_record("commits", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')


def sync_issues(project):
    if is_entity_in_state('issues'):
        url = get_url("issues", project['id'])
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    flatten_id(row, "author")
                    flatten_id(row, "assignee")
                    flatten_id(row, "milestone")
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["issues"]["schema"])

                    if row["updated_at"] >= get_start("project_{}".format(project["id"])):
                        singer.write_record("issues", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')


def sync_milestones(entity, element="project"):
    if is_entity_in_state('milestones'):
        url = get_url(element + "_milestones", entity['id'])

        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES[element + "_milestones"]["schema"])

                    if row["updated_at"] >= get_start(element + "_{}".format(entity["id"])):
                        singer.write_record(element + "_milestones", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')

def sync_users(project):
    if is_entity_in_state('users'):
        url = get_url("users", project['id'])
        project["users"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["users"]["schema"])
                    project["users"].append(row["id"])
                    singer.write_record("users", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')

def sync_deployments(project):
    if is_entity_in_state('deployments'):
        url = get_url("deployments", project['id'])
        project["deployments"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["deployments"]["schema"])
                    project["deployments"].append(row["id"])
                    singer.write_record("deployments", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')

def sync_pipelines(project):
    if is_entity_in_state('pipelines'):
        url = get_url("pipelines", project['id'])
        project["pipelines"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["pipelines"]["schema"])
                    project["pipelines"].append(row["id"])
                    singer.write_record("pipelines", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')

def sync_releases(project):
    if is_entity_in_state('releases'):
        url = get_url("releases", project['id'])
        project["releases"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["releases"]["schema"])
                    project["releases"].append(row["tag_name"])
                    singer.write_record("releases", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading data failed')

def sync_group(gid, pids):
    url = CONFIG['api_url'] + RESOURCES["groups"]['url'].format(gid)

    data = request(url).json()
    time_extracted = utils.now()

    with Transformer(pre_hook=format_timestamp) as transformer:
        group = transformer.transform(data, RESOURCES["groups"]["schema"])

    if not pids:
        #  Get all the projects of the group if none are provided
        for project in group['projects']:
            if project['id']:
                pids.append(project['id'])

    for pid in pids:
        sync_project(pid)

    sync_milestones(group, "group")

    singer.write_record("groups", group, time_extracted=time_extracted)


def sync_project(pid):
    url = get_url("projects", pid)
    data = request(url).json()
    time_extracted = utils.now()

    with Transformer(pre_hook=format_timestamp) as transformer:
        flatten_id(data, "owner")
        add_extraction_date(data)
        project = transformer.transform(data, RESOURCES["projects"]["schema"])

    state_key = "project_{}".format(project["id"])

    #pylint: disable=maybe-no-member
    last_activity_at = project.get('last_activity_at', project.get('created_at'))
    if not last_activity_at:
        raise Exception(
            #pylint: disable=line-too-long
            "There is no last_activity_at or created_at field on project {}. This usually means I don't have access to the project."
            .format(project['id']))


    if project['last_activity_at'] >= get_start(state_key):

        sync_branches(project)
        sync_commits(project)
        sync_issues(project)
        sync_milestones(project)
        sync_users(project)
        sync_deployments(project)
        sync_pipelines(project)
        sync_releases(project)

        singer.write_record("projects", project, time_extracted=time_extracted)
        utils.update_state(STATE, state_key, last_activity_at)
        singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    gids = list(filter(None, CONFIG['groups'].split(' ')))
    pids = list(filter(None, CONFIG['projects'].split(' ')))

    for resource, config in RESOURCES.items():
        singer.write_schema(resource, config['schema'], config['key_properties'])

    for gid in gids:
        sync_group(gid, pids)

    if not gids:
        # When not syncing groups
        for pid in pids:
            sync_project(pid)

    LOGGER.info("Sync complete")


def main_impl():
    # TODO: Address properties that are required or not

    args = utils.parse_args(["private_token", "projects", "start_date"])

    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
