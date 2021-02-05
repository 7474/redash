import requests
import time
import logging
from datetime import datetime
from dateutil import parser
from redash.query_runner import BaseQueryRunner, register, TYPE_DATETIME, TYPE_STRING, TYPE_FLOAT
from redash.utils import json_dumps


class Mackerel(BaseQueryRunner):
    should_annotate_query = False

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'api_key': {
                    'type': 'string',
                    'title': 'API-Key'
                },
                'base_url': {
                    'type': 'string',
                    "default": 'https://api.mackerelio.com',
                    'title': 'Base URL'
                },
            },
            "order": ['api_key', 'base_url'],
            'required': ['api_key'],
            'secret': ['api_key'],
        }

    def test_connection(self):
        api_key = str(self.configuration.get('api_key'))
        headers = {'X-Api-Key': api_key}
        resp = requests.get(self.configuration.get("base_url", None), headers=headers)
        return resp.ok

    def get_schema(self, get_stats=False):
        base_url = self.configuration["base_url"]
        api_key = str(self.configuration.get('api_key'))
        headers = {'X-Api-Key': api_key}
        metrics_path = "/api/v0/services"
        response = requests.get(base_url + metrics_path, headers=headers)
        response.raise_for_status()
        services = response.json()["services"]

        schema = {}
        for name in services:
            schema[name] = {"name": name, "columns": []}
        return list(schema.values())

    def run_query(self, query, user):
        """
        Ref. https://mackerel.io/ja/api-docs/

        example: host/service metrics
            from=2021-02-01T00:00:00Z
            to=2021-02-01T01:00:00Z
            /hosts/<hostId>/metrics?name=<metricName>
            /services/<serviceName>/metrics?name=<metricName>
        """

        base_url = self.configuration["base_url"]
        api_key = str(self.configuration.get('api_key'))
        headers = {'X-Api-Key': api_key}
        columns = [
            {"friendly_name": "timestamp", "type": TYPE_DATETIME, "name": "timestamp"},
        ]
        results = {}

        try:
            error = None
            query = query.strip()
            queries = query.strip().splitlines()

            api_endpoint = base_url + query

            response = requests.get(api_endpoint, headers=headers)
            response.raise_for_status()

            metrics = response.json()["metrics"]
            logging.info(json_dumps(metrics))

            columns.append(
                {
                    "friendly_name": query,
                    "type": TYPE_FLOAT,
                    "name": query,
                }
            )

            rows = [dict(zip(["timestamp", query], [datetime.fromtimestamp(metric['time']), metric['value']])) for metric in metrics]

            logging.info(json_dumps({"rows": rows, "columns": columns}))
            json_data = json_dumps({"rows": rows, "columns": columns})

        except requests.RequestException as e:
            return None, str(e)

        return json_data, error


register(Mackerel)
