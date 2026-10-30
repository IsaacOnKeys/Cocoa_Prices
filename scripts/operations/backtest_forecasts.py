"""Local revised-history simulation, with explicit availability assumptions; no cloud IO."""

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
from datetime import date, timedelta
from pathlib import Path

MEASURES = ('cocoa_price', 'oil_price', 'precipitation', 'soil_moisture')
LIMITS = (7, 14, 7, 7)
HORIZON = 40


def read_inputs(path, as_of):
    rows = {}
    with path.open(encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ['date', *MEASURES]:
            raise ValueError('Expected date,cocoa_price,oil_price,precipitation,soil_moisture CSV')
        for row in reader:
            day = date.fromisoformat(row['date'])
            values = tuple(float(row[key]) if row[key] else None for key in MEASURES)
            values = tuple(None if index >= 2 and value == -999 else value for index, value in enumerate(values))
            if day.isoformat() != row['date'] or day in rows or day > as_of:
                raise ValueError('Duplicate, future or noncanonical observation date')
            if any(value is not None and not math.isfinite(value) for value in values):
                raise ValueError('Nonfinite input measurement')
            if values[0] is not None and values[0] <= 0:
                raise ValueError('Cocoa prices must be positive')
            rows[day] = values
    if not rows:
        raise ValueError('Empty evaluation input')
    return rows


def available_calendar(rows, as_of, lags):
    """At day d, expose only observations dated <= d - assumed publication lag."""
    start = min(rows)
    last = [None]*4
    calendar = {}
    for offset in range((as_of-start).days+1):
        day = start+timedelta(days=offset)
        values = []
        for index, (lag, limit) in enumerate(zip(lags, LIMITS)):
            observed = day-timedelta(days=lag)
            value = rows.get(observed, (None,)*4)[index]
            if value is not None:
                last[index] = (observed, value)
            known = last[index]
            values.append(known[1] if known and (day-known[0]).days <= limit else None)
        calendar[day] = values
    return calendar


def features(calendar):
    vectors = {}
    for day, values in sorted(calendar.items()):
        lags = [calendar.get(day-timedelta(days=lag), [None]*4)[0] for lag in (1, 7, 28)]
        averages = []
        for index in (1, 2, 3):
            window = [calendar[day-timedelta(days=offset)][index] for offset in range(7)
                      if day-timedelta(days=offset) in calendar]
            present = [value for value in window if value is not None]
            averages.append(sum(present)/len(present) if present else None)
        vectors[day] = [values[0], *lags, *averages, str((day.weekday()+1)%7+1), str(day.month)]
    return vectors


def training_dates(rows, vectors, cutoff, cocoa_lag):
    # A target's observation date is not its assumed availability date.
    return [day for day in sorted(vectors)
            if day+timedelta(days=HORIZON+cocoa_lag) <= cutoff
            and rows.get(day+timedelta(days=HORIZON), (None,))[0] is not None]


def ridge_model():
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric = Pipeline([('impute', SimpleImputer(strategy='median', keep_empty_features=True)),
                        ('scale', StandardScaler())])
    preprocess = ColumnTransformer([('numeric', numeric, list(range(7))),
                                    ('calendar', OneHotEncoder(handle_unknown='ignore', sparse_output=False), [7, 8])])
    return Pipeline([('features', preprocess), ('ridge', Ridge(alpha=1.0))])


def score(actual, predicted):
    if not actual:
        return {'n': 0, 'mae': None, 'rmse': None, 'mape_percent': None}
    errors = [prediction-value for value, prediction in zip(actual, predicted)]
    return {'n': len(actual), 'mae': sum(abs(error) for error in errors)/len(errors),
            'rmse': math.sqrt(sum(error*error for error in errors)/len(errors)),
            'mape_percent': 100*sum(abs(error)/value for error, value in zip(errors, actual))/len(errors)}


def simulate(rows, as_of, start, end, step=30, minimum_training=365, lags=(1, 3, 3, 3), model_factory=ridge_model):
    if (len(lags) != 4 or start > end or end+timedelta(days=HORIZON+lags[0]) > as_of
            or step < 1 or minimum_training < 2
            or any(not 0 <= lag <= limit for lag, limit in zip(lags, LIMITS))):
        raise ValueError('Invalid period, training minimum, publication lag or immature evaluation end')
    calendar = available_calendar(rows, as_of, lags)
    vectors = features(calendar)
    predictions, skipped = [], []
    for offset in range(0, (end-start).days+1, step):
        origin = start+timedelta(days=offset)
        if origin.weekday() > 4:
            skipped.append({'origin': origin.isoformat(), 'reason': 'non_operating_day'})
            continue
        target = origin+timedelta(days=HORIZON)
        actual = rows.get(target, (None,))[0]
        eligible = training_dates(rows, vectors, origin, lags[0])
        # Most recent same-weekday observation as the target that could be known at origin.
        seasonal_day = target-timedelta(days=7*math.ceil((HORIZON+lags[0])/7))
        seasonal = calendar.get(seasonal_day+timedelta(days=lags[0]), [None])[0]
        current = vectors.get(origin)
        reason = ('no_observed_target' if actual is None else
                  'insufficient_training' if len(eligible) < minimum_training else
                  'missing_baseline' if current is None or current[0] is None or seasonal is None else None)
        if reason:
            skipped.append({'origin': origin.isoformat(), 'reason': reason})
            continue
        model = model_factory()
        # None becomes NaN in numeric columns; all preprocessing fits only these training rows.
        matrix = [[float('nan') if value is None else value for value in vectors[day]] for day in eligible]
        labels = [rows[day+timedelta(days=HORIZON)][0] for day in eligible]
        model.fit(matrix, labels)
        predicted = float(model.predict([[float('nan') if value is None else value for value in current]])[0])
        if not math.isfinite(predicted):
            raise ValueError('Local candidate produced a nonfinite price; evaluation failed')
        predictions.append({'origin': origin.isoformat(), 'target': target.isoformat(),
                            'training_rows': len(eligible), 'latest_training_origin': eligible[-1].isoformat(),
                            'latest_training_target': (eligible[-1]+timedelta(days=HORIZON)).isoformat(),
                            'actual': actual, 'local_ridge': predicted,
                            'forecast_valid': predicted > 0,
                            'persistence': current[0], 'weekly_seasonal': seasonal})
    actuals = [row['actual'] for row in predictions]
    return {'evaluation_kind': 'revised_history_simulation', 'historical_vintages_verified': False,
            'model_kind': 'local_ridge_v1_not_bigquery_model_parity',
            'status': ('invalid_candidate_predictions' if any(not row['forecast_valid'] for row in predictions)
                       else 'evaluated' if predictions else 'insufficient_eligible_samples'),
            'invalid_forecast_count': sum(not row['forecast_valid'] for row in predictions),
            'horizon_calendar_days': HORIZON, 'as_of': as_of.isoformat(),
            'period': {'start': start.isoformat(), 'end': end.isoformat(), 'step_days': step},
            'assumed_publication_lags_days': dict(zip(MEASURES, lags)),
            'maximum_observation_ages_days': dict(zip(MEASURES, LIMITS)),
            'minimum_training_rows': minimum_training, 'ridge_alpha': 1.0,
            'label_policy': 'observed_target_only; target + assumed cocoa lag <= training cutoff',
            'metrics': {name: score(actuals, [row[name] for row in predictions])
                        for name in ('local_ridge', 'persistence', 'weekly_seasonal')},
            'predictions': predictions, 'skipped': skipped}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--as-of', required=True, type=date.fromisoformat)
    parser.add_argument('--start', required=True, type=date.fromisoformat)
    parser.add_argument('--end', required=True, type=date.fromisoformat)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--step-days', type=int, default=30)
    parser.add_argument('--minimum-training', type=int, default=365)
    parser.add_argument('--publication-lags', type=int, nargs=4, default=(1, 3, 3, 3),
                        metavar=('COCOA', 'OIL', 'RAIN', 'SOIL'))
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Refusing to overwrite an evaluation report')
    rows = read_inputs(args.input, args.as_of)
    report = simulate(rows, args.as_of, args.start, args.end, args.step_days,
                      args.minimum_training, tuple(args.publication_lags))
    report['input_sha256'] = hashlib.sha256(args.input.read_bytes()).hexdigest()
    metadata_path = args.input.with_suffix('.metadata.json')
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
        if metadata.get('input_sha256') != report['input_sha256']:
            raise ValueError('Input no longer matches the exported snapshot metadata')
        report['source_snapshot'] = metadata
    report['code_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report['dependencies'] = {name: importlib.metadata.version(name) for name in ('numpy', 'scipy', 'scikit-learn')}
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False), encoding='utf-8')
    print(json.dumps({'status': report['status'], 'metrics': report['metrics'], 'output': str(args.output)}))


if __name__ == '__main__':
    main()
