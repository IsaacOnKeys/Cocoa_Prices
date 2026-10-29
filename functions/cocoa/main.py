import json
import os
import re
import time
from datetime import UTC, date, datetime

import functions_framework
import requests
from bs4 import BeautifulSoup, FeatureNotFound
from google.cloud import pubsub_v1
from request_policy import INVOCATION_SECONDS, acquisition_identity, coverage, get, post, publish_batch

URL = "https://www.icco.org/home/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = os.getenv("COCOA_TOPIC", "cocoa-prices-topic")
SCHEMA_FILE = "cocoa_schema.avsc"  # must be in the same folder as this script
HISTORY_URL = "https://www.icco.org/statistics/"
HISTORY_QUERY = "https://www.icco.org/wp-admin/admin-ajax.php?action=get_wdtable&table_id=26"


def safe_float(x):
    """Convert to float, but return None if NaN or Infinity."""
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def _scrape_price(*, deadline=None) -> tuple[date, float]:
    print(f"Fetching ICCO cocoa price from {URL}")
    deadline = time.monotonic() + 40 if deadline is None else deadline
    html = get(requests, URL, headers=HEADERS, deadline=deadline, label="ICCO").text
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")

    rows = soup.select("table#table_1 tbody tr[id^=table_]")
    if not rows:
        raise RuntimeError("price rows not found")

    latest_date, latest_price = None, None
    for r in rows:
        tds = r.find_all("td")
        if len(tds) < 6:
            continue
        try:
            date_val = datetime.strptime(tds[1].text.strip(), "%d/%m/%Y").date()
        except ValueError:
            continue
        try:
            price_val = safe_float(re.sub(",", "", tds[-1].text.strip()))
        except Exception:
            continue
        if latest_date is None or date_val > latest_date:
            latest_date, latest_price = date_val, price_val

    if latest_price is None:
        raise RuntimeError("no valid price found")

    print(f"Latest cocoa price: {latest_price} on {latest_date}")
    return latest_date, latest_price


def fetch_window(start, end, *, deadline):
    """Read the ordinary public daily-price table, newest first, to the boundary."""
    try:
        headers = {**HEADERS, 'X-Requested-With': 'XMLHttpRequest', 'Referer': HISTORY_URL}
        html = get(requests, HISTORY_URL, headers=headers, deadline=deadline, label='ICCO statistics').text
        soup = BeautifulSoup(html, 'html.parser')
        node = soup.find(id='table_1_desc')
        config = json.loads(node.get('value') or node.get_text())
        columns = [h.get_text(' ', strip=True) for h in soup.select('table#table_1 thead th')]
        if config['tableWpId'] != 26 or len(columns) != 6 or columns[1] != 'Date' or 'Euro/tonne' not in columns[-1]:
            raise ValueError('Unexpected ICCO series/units')
        if config['dataTableParams']['ajax']['url'] != HISTORY_QUERY or 100 not in config['dataTableParams']['aLengthMenu'][0]:
            raise ValueError('Unexpected ICCO pagination interface')
        nonce = soup.find(id='wdtNonceFrontendServerSide_26')['value']
        records, total, previous, reached_start = {}, None, None, False
        for page in range(10):
            body = {'draw': page+1, 'start': page*100, 'length': 100, 'search[value]': '', 'search[regex]': 'false',
                    'order[0][column]': 1, 'order[0][dir]': 'desc', 'wdtNonce': nonce}
            for i in range(6):
                for key, value in [('data', i), ('name', ''), ('searchable', 'true'), ('orderable', 'true'),
                                   ('search][value', ''), ('search][regex', 'false')]:
                    body[f'columns[{i}][{key}]'] = value
            response = post(requests, HISTORY_QUERY, data=body, headers=headers, deadline=deadline, label='ICCO daily table')
            document = response.json()
            count = int(document['recordsTotal'])
            if count != int(document['recordsFiltered']) or not 0 < count <= 100_000 or (total is not None and total != count):
                raise ValueError('ICCO table changed during pagination')
            total = count
            batch = document.get('data', document.get('aaData'))
            if not isinstance(batch, list) or len(batch) != min(100, total-page*100):
                raise ValueError('Incomplete ICCO page')
            for row in batch:
                if not isinstance(row, list) or len(row) != 6:
                    raise ValueError('Unexpected ICCO row')
                if not any(str(v).strip() for v in row[1:]):
                    continue  # Exposed entirely blank rows are not dated observations.
                observed = datetime.strptime(row[1], '%d/%m/%Y').date()
                if observed.strftime('%d/%m/%Y') != row[1] or observed > end or (previous is not None and observed >= previous):
                    raise ValueError('Invalid, duplicate or unordered ICCO date')
                previous = observed
                if observed < start:
                    reached_start = True
                    continue
                value = safe_float(row[-1].replace(',', ''))
                if value is None or value <= 0:
                    raise ValueError('Invalid ICCO measurement')
                records[observed] = value
            if reached_start:
                break
            if (page+1)*100 >= total:
                raise ValueError('ICCO history no longer covers recovery boundary')
        if not reached_start or not records:
            raise ValueError('Incomplete ICCO recovery window')
        # Homepage can expose several quotes before the statistics table updates.
        home = get(requests, URL, headers=HEADERS, deadline=deadline, label='ICCO').text
        home_rows = BeautifulSoup(home, 'html.parser').select('table#table_1 tbody tr[id^=table_]')
        home_dates = set()
        for row in home_rows:
            cells = row.find_all('td')
            if len(cells) != 6:
                raise ValueError('Unexpected ICCO homepage row')
            observed = datetime.strptime(cells[1].get_text(strip=True), '%d/%m/%Y').date()
            value = safe_float(cells[-1].get_text(strip=True).replace(',', ''))
            if observed > end or observed in home_dates or value is None or value <= 0:
                raise ValueError('Invalid ICCO homepage observation')
            home_dates.add(observed)
            if observed >= start:
                if observed in records and records[observed] != value:
                    raise ValueError('ICCO publisher interfaces disagree')
                records[observed] = value
        if not any(d >= start for d in home_dates):
            raise ValueError('No relevant ICCO homepage observations')
        return sorted(records.items())
    except Exception:
        raise RuntimeError('Invalid or incomplete ICCO daily window') from None


@functions_framework.http
def publish_price(request):
    if request is not None and request.method == "HEAD":
        return "", 204
    deadline = time.monotonic() + INVOCATION_SECONDS
    try:
        acquisition_id = acquisition_identity("ICCO", key=request.headers.get("X-Acquisition-Key") if request is not None else None)
        progress = coverage('ICCO', datetime.now(UTC).date(), deadline=deadline)
        observations = fetch_window(progress.start, progress.end, deadline=deadline-10) if progress else [_scrape_price(deadline=deadline-10)]
    except Exception:
        print("[FAIL] ICCO acquisition failed")
        return (
            json.dumps({"error": "ICCO acquisition failed"}),
            500,
            {"Content-Type": "application/json"},
        )

    now = datetime.now(UTC)

    # Load Avro schema from file in function directory
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = json.load(f)

    try:
        records = []
        for observation_date, cocoa_price in observations:
            payload = {'date': observation_date.isoformat(), 'cocoa_price': cocoa_price, 'ingestion_time': now.isoformat(),
                       'raw_payload': json.dumps({'date': observation_date.isoformat(), 'price': cocoa_price,
                                                  'date_basis': 'source_observation'}, separators=(',', ':'))}
            records.append(payload)
        archive = publish_batch(pubsub_v1.PublisherClient, PROJECT, TOPIC, records, schema, source='ICCO', acquisition_id=acquisition_id,
                                acquired_at=now.isoformat(), deadline=deadline, progress=progress)
        receipt = {'source': 'ICCO', 'status': 'published', 'published': len(observations),
                   'observation_start': observations[0][0].isoformat(), 'observation_end': observations[-1][0].isoformat(),
                   'acquired_at': now.isoformat(), 'acquisition_id': acquisition_id, **archive}
        if progress:
            receipt.update(progress.complete(receipt, [{'date': d.isoformat(), 'cocoa_price': v} for d, v in observations]))
    except Exception:
        print("[FAIL] Cocoa Avro publication failed")
        return (
            json.dumps({"error": "Cocoa Avro publication failed"}),
            500,
            {"Content-Type": "application/json"},
        )

    print(json.dumps({'event': 'acquisition_complete', **receipt}))
    return (
        json.dumps({"date": payload["date"], "price": cocoa_price}),
        200,
        {"Content-Type": "application/json"},
    )


if __name__ == "__main__":
    publish_price(None)
