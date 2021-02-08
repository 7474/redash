import requests
import logging
import re
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
        try:
            base_url = self.configuration["base_url"]
            api_key = str(self.configuration.get('api_key'))
            headers = {'X-Api-Key': api_key}

            schema = {}

            # https://mackerel.io/ja/api-docs/entry/services
            response = requests.get(base_url + "/api/v0/services", headers=headers)
            response.raise_for_status()
            services = response.json()["services"]
            for service in services:
                service_name = service["name"]
                response = requests.get(base_url + "/api/v0/services/{}/metric-names".format(service_name), headers=headers)
                response.raise_for_status()
                names = response.json()["names"]
                columns = ["/services/{}/metrics?name={}".format(service_name, name) for name in names]
                schema[service_name] = {"name": service_name, "columns": columns}

            # https://mackerel.io/ja/api-docs/entry/hosts
            response = requests.get(base_url + "/api/v0/hosts", headers=headers)
            response.raise_for_status()
            hosts = response.json()["hosts"]
            for host in hosts:
                host_id = host["id"]
                host_name = host["name"]
                response = requests.get(base_url + "/api/v0/hosts/{}/metric-names".format(host_id), headers=headers)
                response.raise_for_status()
                names = response.json()["names"]
                columns = ["/hosts/{}/metrics?name={}".format(host_id, name) for name in names]
                schema[host_id] = {"name": host_name, "columns": columns}

            logging.info(json_dumps(schema))
            return list(schema.values())
        except requests.RequestException as e:
            logging.error(e)
            raise 

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
        metric_names = []
        results = {}

        try:
            error = None
            queries = query.strip().splitlines()
            q_to = int(datetime.now().timestamp())
            q_from = q_to - 60 * 60

            for query in queries:
                if query.isspace():
                    continue
                m = re.match(r'from=(\d+)$', query)
                if m:
                    q_from = m.groups()[0]
                    continue
                m = re.match(r'to=(\d+)$', query)
                if m:
                    q_to = m.groups()[0]
                    continue
                m = re.match(r'from=(.+)', query)
                if m:
                    q_from = int(parser.parse(m.groups()[0]).timestamp())
                    continue
                m = re.match(r'to=(.+)', query)
                if m:
                    q_to = int(parser.parse(m.groups()[0]).timestamp())
                    continue

                metric_name = query.replace('metrics?name=', '')
                api_endpoint = base_url + "/api/v0{}&from={}&to={}".format(query, q_from, q_to)

                response = requests.get(api_endpoint, headers=headers)
                response.raise_for_status()

                metrics = response.json()["metrics"]
                logging.info(json_dumps(metrics))

                columns.append(
                    {
                        "friendly_name": query,
                        "type": TYPE_FLOAT,
                        "name": metric_name,
                    }
                )
                metric_names.append(metric_name)
                for metric in metrics:
                    if metric['time'] not in results:
                        results[metric['time']] = {}
                    results[metric['time']][metric_name] = metric['value']

            rows = []
            last_value_map = dict(zip(metric_names, [0 for m in metric_names]))
            for time in sorted(results.keys()):
                row = {"timestamp": datetime.fromtimestamp(time)}
                for metric_name in metric_names:
                    if metric_name in results[time]:
                        row[metric_name] = results[time][metric_name]
                        last_value_map[metric_name] = results[time][metric_name]
                    else:
                        # XXX フェイルバック値
                        row[metric_name] = last_value_map[metric_name]
                rows.append(row)

            logging.info(json_dumps({"rows": rows, "columns": columns}))
            json_data = json_dumps({"rows": rows, "columns": columns})

        except requests.RequestException as e:
            return None, str(e)

        return json_data, error


register(Mackerel)
