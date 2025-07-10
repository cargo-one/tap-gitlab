#!/usr/bin/env python3

import datetime
import sys
import os
import requests
import singer
from singer import Transformer, utils, strftime
import logging

import pytz
import backoff
from strict_rfc3339 import rfc3339_to_timestamp

PER_PAGE = 100
CONFIG = {
    'api_url': "https://gitlab.com/api/v4",
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
    'merge_requests': {
        'url': '/projects/{}/merge_requests',
        'schema': load_schema('merge_requests'),
        'key_properties': ['id'],
    },
    'discussions': {
        'url': '/projects/{}/merge_requests/{}/discussions',
        'schema': load_schema('discussions'),
        'key_properties': ['id'],
    },
    'notes': {
        'url': '/projects/{}/merge_requests/{}/notes',
        'schema': load_schema('notes'),
        'key_properties': ['id'],
    },
}


LOGGER = singer.get_logger()
SESSION = requests.Session()




def get_base_url(entity, id, mr_iid=None):
    """Get the base URL without query parameters"""
    if not isinstance(id, int):
        id = id.replace("/", "%2F")

    if entity in ['discussions', 'notes'] and mr_iid is not None:
        return CONFIG['api_url'] + RESOURCES[entity]['url'].format(id, mr_iid)
    else:
        return CONFIG['api_url'] + RESOURCES[entity]['url'].format(id)


def get_date_filter_params(entity, state_key):
    """Get date filter parameters as a dict"""
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
        "merge_requests": "updated_after",
        "discussions": '',
        "notes": '',
    }

    filter_param = date_filtering.get(entity)
    if filter_param and STATE.get(state_key):
        params = {filter_param: STATE.get(state_key)}
        # Deployments API requires order_by and sort parameters when using updated_after
        if entity == "deployments" and filter_param == "updated_after":
            params['order_by'] = 'updated_at'
            params['sort'] = 'asc'
        return params
    else:
        return {}


def get_url(entity, id, mr_iid=None):
    """Get URL with date filter for backward compatibility"""
    state_key = "project_{}".format(id)
    base_url = get_base_url(entity, id, mr_iid)
    date_params = get_date_filter_params(entity, state_key)
    
    if date_params:
        param_str = '&'.join([f'{k}={v}' for k, v in date_params.items()])
        return f'{base_url}?{param_str}'
    else:
        return base_url


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
    LOGGER.debug("GET {}".format(req.url))
    resp = SESSION.send(req)

    if resp.status_code >= 400:
        raise Exception("Error making request to GitLab API: GET {} [{} - {}]".format(
                req.url, resp.status_code, resp.content))

    return resp


def gen_request(url):
    # Parse existing query parameters from URL
    from urllib.parse import urlparse, parse_qs, urlencode
    parsed = urlparse(url)
    existing_params = parse_qs(parsed.query)
    
    # Flatten single-value parameters, skip empty parameter names
    params = {k: v[0] if len(v) == 1 else v for k, v in existing_params.items() if k}
    params['page'] = 1
    
    # Build base URL without query string
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    resp = request(base_url, params)
    last_page = int(resp.headers.get('X-Total-Pages', 1))

    for row in resp.json():
        yield row

    for page in range(2, last_page + 1):
        params['page'] = page
        resp = request(base_url, params)
        for row in resp.json():
            yield row


def alt_gen_request(url):
    # Some endpoints use x-next-page instead of x-total-pages
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    existing_params = parse_qs(parsed.query)
    
    # Flatten single-value parameters, skip empty parameter names
    params = {k: v[0] if len(v) == 1 else v for k, v in existing_params.items() if k}
    params['page'] = 1
    
    # Build base URL without query string
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    resp = request(base_url, params)
    next_page = resp.headers.get('x-next-page', False)

    for row in resp.json():
        yield row

    while next_page:
        params['page'] = next_page
        resp = request(base_url, params)
        next_page = resp.headers.get('x-next-page', False)
        for row in resp.json():
            yield row


def format_timestamp(data, typ, schema):
    result = data
    if typ == 'string' and schema.get('format') == 'date-time' and data is not None:
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


def calculate_mr_metrics(mr_data, project_id):
    """Calculate computed metrics for merge requests"""
    # Initialize computed fields with defaults
    mr_data['commits_count'] = 0
    mr_data['first_review_at'] = None
    mr_data['first_approval_at'] = None
    mr_data['commits_after_first_review'] = 0
    mr_data['has_staging_deployment'] = False
    mr_data['staging_deployment_at'] = None
    
    try:
        # Get commits for this MR
        commits_base_url = get_base_url('commits', project_id)
        commits_url = f"{CONFIG['api_url']}/projects/{project_id}/merge_requests/{mr_data['iid']}/commits"
        commits = list(gen_request(commits_url))
        mr_data['commits_count'] = len(commits)
        
        # Get discussions for review metrics
        discussions_url = f"{CONFIG['api_url']}/projects/{project_id}/merge_requests/{mr_data['iid']}/discussions"
        discussions = list(gen_request(discussions_url))
        
        # Calculate first review/approval times
        first_review_at = None
        first_approval_at = None
        
        for discussion in discussions:
            for note in discussion.get('notes', []):
                if not note.get('system', False) and note.get('author', {}).get('id') != mr_data.get('author', {}).get('id'):
                    note_created = note.get('created_at')
                    if note_created and (not first_review_at or note_created < first_review_at):
                        first_review_at = note_created
                
                # Check for approval indicators
                if 'approved' in note.get('body', '').lower() or note.get('type') == 'approval':
                    note_created = note.get('created_at')
                    if note_created and (not first_approval_at or note_created < first_approval_at):
                        first_approval_at = note_created
        
        mr_data['first_review_at'] = first_review_at
        mr_data['first_approval_at'] = first_approval_at
        
        # Count commits after first review
        commits_after_first_review = 0
        if first_review_at:
            for commit in commits:
                if commit.get('created_at', '') > first_review_at:
                    commits_after_first_review += 1
        
        mr_data['commits_after_first_review'] = commits_after_first_review
        
        # Check for staging deployment pipeline (simplified to avoid too many API calls)
        try:
            pipelines_url = f"{CONFIG['api_url']}/projects/{project_id}/merge_requests/{mr_data['iid']}/pipelines"
            pipelines = list(gen_request(pipelines_url))
            
            has_staging_deployment = False
            staging_deployment_at = None
            
            # Check if any pipeline exists (simplified check)
            if pipelines:
                has_staging_deployment = True
                # Use the latest pipeline timestamp as approximation
                latest_pipeline = max(pipelines, key=lambda p: p.get('created_at', ''))
                staging_deployment_at = latest_pipeline.get('created_at')
            
            mr_data['has_staging_deployment'] = has_staging_deployment
            mr_data['staging_deployment_at'] = staging_deployment_at
            
        except Exception as pipeline_error:
            LOGGER.debug(f"Failed to get pipeline info for MR {mr_data.get('iid')}: {pipeline_error}")
        
    except Exception as e:
        LOGGER.debug(f"Failed to calculate metrics for MR {mr_data.get('iid')}: {e}")
    
    return mr_data


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
            except Exception as e:
                if "403 Forbidden" in str(e):
                    LOGGER.debug(f'Deployments access forbidden for project {project["id"]} - skipping')
                elif "400 Bad request" in str(e):
                    LOGGER.debug(f'Deployments API error for project {project["id"]} - skipping: {e}')
                else:
                    LOGGER.exception('Loading deployments data failed')

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


def sync_merge_requests(project):
    if is_entity_in_state('merge_requests'):
        url = get_url("merge_requests", project['id'])
        project["merge_requests"] = []
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in alt_gen_request(url):
                    # Calculate computed metrics
                    row = calculate_mr_metrics(row, project['id'])
                    
                    # Flatten related objects
                    flatten_id(row, "author")
                    flatten_id(row, "assignee")
                    flatten_id(row, "milestone")
                    
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["merge_requests"]["schema"])
                    
                    # Sync discussions and notes for this MR
                    sync_discussions(project, row['iid'])
                    sync_notes(project, row['iid'])
                    
                    project["merge_requests"].append(row["id"])
                    singer.write_record("merge_requests", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading merge requests data failed')


def sync_discussions(project, mr_iid):
    if is_entity_in_state('discussions'):
        url = get_url("discussions", project['id'], mr_iid)
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    row['project_id'] = project['id']
                    row['merge_request_iid'] = mr_iid
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["discussions"]["schema"])
                    singer.write_record("discussions", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading discussions data failed')


def sync_notes(project, mr_iid):
    if is_entity_in_state('notes'):
        url = get_url("notes", project['id'], mr_iid)
        with Transformer(pre_hook=format_timestamp) as transformer:
            try:
                for row in gen_request(url):
                    row['project_id'] = project['id']
                    row['merge_request_iid'] = mr_iid
                    add_extraction_date(row)
                    transformed_row = transformer.transform(row, RESOURCES["notes"]["schema"])
                    singer.write_record("notes", transformed_row, time_extracted=utils.now())
            except Exception:
                LOGGER.exception('Loading notes data failed')

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
    try:
        data = request(url).json()
    except Exception as e:
        if "404" in str(e):
            LOGGER.debug(f'Project {pid} not found (404) - skipping')
            return
        else:
            raise e
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
        sync_merge_requests(project)

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
    import sys
    
    # Check for version flag
    if len(sys.argv) > 1 and sys.argv[1] == '--version':
        print("tap-gitlab version 1.0.6")
        print("Enhanced with code review metrics")
        return

    args = utils.parse_args(["private_token", "projects", "start_date"])

    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    # Configure logging to reduce third-party library verbosity
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('google.api_core').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    # Log version at startup
    LOGGER.info("Starting tap-gitlab v1.0.6 with code review metrics")
    
    do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
