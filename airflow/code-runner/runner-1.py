#!/usr/bin/env python

from bottle import route, run, request
import ansible_runner
import json
import sys
import os

@route('/job_run/ping.yml', method='POST')
def job_run(playbook):

    extravars = {}

    try:
        extravars = json.loads(request.body.read())
    except Exception as e:
        print(str(e), file=sys.stderr)

    os.environ["ANSIBLE_NOCOLOR"] = "1"
    os.environ["ANSIBLE_NOCOWS"] = "1"

    params = {
        "playbook"  : playbook,
        "extravars" : extravars,
        "json_mode" : False,        # well-known playbook output
        "quiet"     : False,         # no stdout here in runner
    }

    r = ansible_runner.run(private_data_dir='jp/', **params)

    # return the playbook run output
    return open(r.stdout.name, "r").read()

run(host='localhost', port=8081, debug=False)