# -*- encoding: utf-8 -*-
import csv as csv
import json
import requests

from redash.query_runner import BaseQueryRunner, register

# https://discuss.redash.io/t/creating-a-new-query-runner-data-source-in-redash/347
class Mackerel(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'apikey': {
                    'type': 'string',
                    'title': 'API-Key'
                },
            },
            "order": ['apikey'],
            'required': ['apikey'],
            'secret': ['apikey'],
        }

    @classmethod
    def annotate_query(cls):
        return False

    def __init__(self, configuration):
        super(Mackerel, self).__init__(configuration)

    def test_connection(self):
        # TODO なんか取る
        pass

    def run_query(self, query, user):
        # https://ameblo.jp/gcrest-engineer/entry-12339623721.html
        apikey = str(self.configuration.get('apikey'))
        metrics = query.strip().splitlines()

        data = {
            'columns': metrics,
            'rows': [],
        }

        for row in csv.DictReader(query.strip().splitlines(), delimiter=delimiter):
            if len(data['columns']) == 0:
                for key in row.keys():
                    data['columns'].append({'name': key, 'friendly_name': key})

            data['rows'].append(row)

        return json.dumps(data), None

        response = requests.get('https://api.mackerelio.com/api/v0/services/<serviceName>/metrics', headers={'X-Api-Key': apikey})
        response_json = response.json()
        data = {
            'columns': [
                {'name': 'id', 'friendly_name': 'id'},
                {'name': 'status', 'friendly_name': 'status'},
                {'name': 'monitorId', 'friendly_name': 'monitorId'},
                {'name': 'hostId', 'friendly_name': 'hostId'},
                {'name': 'value', 'friendly_name': 'value'},
                {'name': 'message', 'friendly_name': 'message'},
                {'name': 'reason', 'friendly_name': 'reason'},
                {'name': 'openedAt', 'friendly_name': 'openedAt'},
                {'name': 'closedAt', 'friendly_name': 'closedAt'},
            ],
            'rows': response_json['alerts'],
        }

        return json.dumps(data), None

register(Mackerel)
