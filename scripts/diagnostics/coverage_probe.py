"""Private validation entry point, packaged with byte-identical producer modules."""

import base64
import importlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from google.cloud import storage
from request_policy import coverage

SOURCES = {'cocoa': 'ICCO', 'oil': 'EIA', 'weather': 'NASA_POWER'}


def validate_coverage(event, context):
    body = json.loads(base64.b64decode(event['data'], validate=True))
    feed, case = body['feed'], body['case']
    if feed not in SOURCES or case not in {'live', 'fixture_initial', 'fixture_corrected', 'source_failure', 'partial_publish'}:
        raise RuntimeError('Invalid isolated coverage scenario')
    token = os.environ['PROBE_TOKEN']
    module = importlib.reload(importlib.import_module(feed + '_producer'))
    today = datetime.now(timezone.utc).date()
    logical_case = 'revision' if case.startswith('fixture_') else case
    key = token + ':' + feed + ':' + logical_case
    probe_publish = {'attempts': 0}
    if case != 'live':
        window = coverage(SOURCES[feed], today, deadline=time.monotonic()+50)
        dates = [window.start + timedelta(days=i) for i in range((window.end-window.start).days+1)]
        target = today-timedelta(days=20)
        while target.weekday() > 4:
            target -= timedelta(days=1)
        correction = case == 'fixture_corrected'

        def value(observed):
            return float((5000 if feed == 'cocoa' else 70) + (1 if correction and observed == target else 0))

        if feed == 'cocoa':
            module.fetch_window = lambda *args, **kwargs: [(d, value(d)) for d in dates if d.weekday() < 5]
        else:
            original_requests = module.requests

            def get(url, **kwargs):
                if feed == 'weather':
                    query = parse_qs(urlparse(url).query)
                    assert query['start'] == [window.start.strftime('%Y%m%d')]
                    values = {d.strftime('%Y%m%d'): (2.5 + (1 if correction and d == target else 0)) for d in dates}
                    moistures = {d.strftime('%Y%m%d'): (-999 if d == today else 0.4) for d in dates}
                    return SimpleNamespace(status_code=200, json=lambda: {'header': {'time_standard': 'LST'},
                        'properties': {'parameter': {'PRECTOTCORR': values, 'GWETROOT': moistures}}})
                if '/v2/' in url:
                    assert kwargs['params']['start'] == window.start.isoformat()
                    rows = [{'period': d.isoformat(), 'series': 'RBRTE', 'units': '$/BBL', 'value': str(value(d))}
                            for d in dates if d.weekday() < 5]
                    return SimpleNamespace(status_code=200, json=lambda: {'response': {'total': str(len(rows)), 'data': rows}})
                monday = window.start-timedelta(days=window.start.weekday())
                markup = '<title>Europe Brent Spot Price FOB Dollars per Barrel</title><table>'
                while monday <= window.end:
                    label = f'{monday.year} {monday.strftime("%b")}-{monday.day} to end'
                    markup += '<tr><td>'+label+'</td>'+''.join(
                        '<td>'+str(value(monday+timedelta(days=i)))+'</td>' for i in range(5))+'</tr>'
                    monday += timedelta(days=7)
                return SimpleNamespace(status_code=200, text=markup+'</table>')

            module.requests = SimpleNamespace(get=get, RequestException=original_requests.RequestException)
        if case == 'source_failure':
            def fail(*args, **kwargs):
                raise RuntimeError('Controlled source failure')
            if feed == 'cocoa':
                module.fetch_window = fail
            elif feed == 'oil':
                module.fetch_observations = fail
            else:
                module.requests.get = fail
        if case == 'partial_publish':
            real = module.pubsub_v1.PublisherClient()

            class Publisher:
                topic_path = staticmethod(real.topic_path)
                count = 0

                def publish(self, *args, **kwargs):
                    self.count += 1
                    probe_publish['attempts'] = self.count
                    if self.count == 2:
                        def fail(**kwargs):
                            raise RuntimeError('Controlled publication failure')
                        return SimpleNamespace(result=fail)
                    return real.publish(*args, **kwargs)

            module.pubsub_v1 = SimpleNamespace(PublisherClient=Publisher)
    client = storage.Client()
    bucket = client.bucket(os.environ['COVERAGE_STATE_BUCKET'])
    path = f'acquisition-coverage/v1/{SOURCES[feed]}/state.json'

    def read_state():
        return json.loads(bucket.blob(path).download_as_bytes(timeout=10, retry=None))

    before = read_state()
    accepted, error = False, None
    try:
        if feed == 'cocoa':
            result = module.publish_price(SimpleNamespace(method='POST', headers={'X-Acquisition-Key': key}))
            accepted = result[1] == 200
        else:
            producer = getattr(module, 'publish_eia_data' if feed == 'oil' else 'publish_weather_data')
            producer({'attributes': {'acquisition_key': key}}, context)
            accepted = True
    except Exception:
        error = 'controlled_or_source_failure'  # Never persist transport/credential exceptions.
    after = read_state()
    result = {'feed': feed, 'case': case, 'accepted': accepted, 'before': before, 'after': after, 'error': error,
              'target_date': target.isoformat() if case != 'live' else None,
              'controlled_publish_attempts': probe_publish['attempts']}
    if accepted:
        receipt = client.bucket(os.environ['COVERAGE_RECEIPT_BUCKET']).blob(after['receipt_object'])
        result['receipt'] = json.loads(receipt.download_as_bytes(timeout=10, retry=None))
    object_name = 'probe-results/' + feed + '/' + case + '.json'
    bucket.blob(object_name).upload_from_string(json.dumps(result), if_generation_match=0, timeout=10, retry=None)
    print(json.dumps({'event': 'coverage_probe_complete', 'feed': feed, 'case': case, 'accepted': accepted}))
