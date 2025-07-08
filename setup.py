#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gitlab',
      version='1.0.2',
      description='Singer.io tap for extracting data from the GitLab API with code review metrics',
      author='Stitch',
      url='https://singer.io',
      classifiers=[
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.12'
      ],
      python_requires='>=3.8',
      py_modules=['tap_gitlab'],
      install_requires=[
          'singer-python>=6.1.1',
          'requests>=2.32.0',
          'strict-rfc3339>=0.7',
          'backoff>=2.2.1'
      ],
      entry_points='''
          [console_scripts]
          tap-gitlab=tap_gitlab:main
      ''',
      packages=['tap_gitlab'],
      package_data = {
          'tap_gitlab/schemas': [
            "branches.json",
            "commits.json",
            "deployments.json",
            "discussions.json",
            "groups.json",
            "issues.json",
            "merge_requests.json",
            "milestones.json",
            "notes.json",
            "pipelines.json",
            "projects.json",
            "releases.json",
            "users.json",
          ],
      },
      include_package_data=True,
)

