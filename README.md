# tap-gitlab

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from GitLab's [REST API](https://docs.gitlab.com/ee/api/README.html)
- Extracts the following resources from GitLab:
  - [Branches](https://docs.gitlab.com/ee/api/branches.html)
  - [Commits](https://docs.gitlab.com/ee/api/commits.html)
  - [Issues](https://docs.gitlab.com/ee/api/issues.html)
  - [Projects](https://docs.gitlab.com/ee/api/projects.html)
  - [Project milestones](https://docs.gitlab.com/ee/api/milestones.html)
  - [Users](https://docs.gitlab.com/ee/api/users.html)
  - [Groups](https://docs.gitlab.com/ee/api/group_milestones.html)
  - [Group Milestones](https://docs.gitlab.com/ee/api/users.html)
  - [Deployments](https://docs.gitlab.com/ee/api/deployments.html)
  - [Pipelines](https://docs.gitlab.com/ee/api/pipelines.html)
  - [Releases](https://docs.gitlab.com/ee/api/releases/index.html)
  - **[Merge Requests](https://docs.gitlab.com/ee/api/merge_requests.html)** - *NEW*
  - **[Discussions](https://docs.gitlab.com/ee/api/discussions.html)** - *NEW*
  - **[Notes](https://docs.gitlab.com/ee/api/notes.html)** - *NEW*
- **Code Review Metrics**: Calculates comprehensive review analytics including review ratios, speed, depth, and MR quality
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## ✨ Code Review Features

This enhanced version provides built-in calculation of key code review metrics:

- **Review Ratio**: Reviews given vs MRs created per developer
- **Review Speed**: Time from MR creation to first review/approval  
- **Review Depth**: Comments per 100 lines of code changed
- **MR Quality**: Commits made after initial review feedback
- **Staging Deployment**: Pipeline execution tracking before merge

### Computed Fields

New merge request records include these computed metrics:
- `first_review_at` - Timestamp of first review comment  
- `first_approval_at` - Timestamp of first approval
- `commits_after_first_review` - Count of commits after review feedback
- `has_staging_deployment` - Boolean flag for staging pipeline execution
- `staging_deployment_at` - Timestamp of successful staging deployment


## Quick start

1. Install

    ```bash
    > pip install tap-gitlab
    ```

2. Get your GitLab access token

    - Login to your GitLab account
    - Navigate to your profile page
    - Create an access token

3. Create the config file

    Create a JSON file called `config.json` containing:
    - Access token you just created
    - API URL for your GitLab account. If you are using the public gitlab.com this will be `https://gitlab.com/api/v4`
    - Groups to track (space separated)    
    - Projects to track (space separated)
    
    Notes:
    - either groups or projects need to be provided
    - filling in 'groups' but leaving 'projects' empty will sync all group projects.
    - filling in 'projects' but leaving 'groups' empty will sync selected projects.
    - filling in 'groups' and 'groups' will sync selected projects of those groups.

    ```json
    {"api_url": "https://gitlab.com/api/v4",
     "private_token": "your-access-token",
    "groups": "myorg mygroup", 
    "projects": "myorg/repo-a myorg/repo-b",
     "start_date": "2018-01-01T00:00:00Z"}
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all GitLab data

    ```json
    {"branches": "2017-01-17T00:00:00Z",
    "commits": "2017-01-17T00:00:00Z",
    "issues": "2017-01-17T00:00:00Z",
    "projects": "2017-01-17T00:00:00Z",
    "project_milestones": "2017-01-17T00:00:00Z", 
    "users": "2017-01-17T00:00:00Z",
    "group_milestones": "2017-01-17T00:00:00Z",
    "deployments": "2017-01-17T00:00:00Z",
    "pipelines": "2017-01-17T00:00:00Z",
    "releases": "2017-01-17T00:00:00Z",
    "merge_requests": "2017-01-17T00:00:00Z",
    "discussions": "2017-01-17T00:00:00Z",
    "notes": "2017-01-17T00:00:00Z"}
    ```
    
    Note:
    - currently, groups don't have a date field which can be tracked

5. Run the application

    `tap-gitlab` can be run with:

    ```bash
    tap-gitlab --config config.json [--state state.json]
    ```

---

Copyright &copy; 2018 Stitch
