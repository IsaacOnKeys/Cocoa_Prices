"""Chronology, publication assumptions and paired baselines without cloud/sklearn IO."""

import math
import runpy
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
M = runpy.run_path(str(ROOT/'scripts/operations/backtest_forecasts.py'))


def observations():
    start = date(2018, 1, 1)
    return {start+timedelta(days=i): (1000.0+i, 10.0+i/100, 1.0, 0.5) for i in range(1000)}


class Model:
    def fit(self, x, y):
        self.mean = sum(y)/len(y)
        self.training_count = len(x)
        return self

    def predict(self, x):
        return [self.mean+x[0][0]/100]


def evaluate(rows, **kwargs):
    return M['simulate'](rows, date(2020, 9, 26), date(2020, 1, 6), date(2020, 1, 6),
                         minimum_training=30, model_factory=Model, **kwargs)


def test_training_includes_only_labels_available_at_the_origin():
    report = evaluate(observations())
    row = report['predictions'][0]
    assert date.fromisoformat(row['latest_training_target'])+timedelta(days=1) <= date.fromisoformat(row['origin'])
    assert date.fromisoformat(row['latest_training_origin'])+timedelta(days=40) == date.fromisoformat(row['latest_training_target'])
    assert row['target'] == '2020-02-15'
    assert report['historical_vintages_verified'] is False
    assert report['evaluation_kind'] == 'revised_history_simulation'


def test_future_data_cannot_change_training_prediction_or_baselines():
    rows = observations()
    before = evaluate(rows)['predictions'][0]
    for day in rows:
        if day > date(2020, 1, 6):
            rows[day] = (9999999.0, 9999999.0, 9999999.0, 9999999.0)
    after = evaluate(rows)['predictions'][0]
    for key in ('local_ridge', 'persistence', 'weekly_seasonal', 'training_rows', 'latest_training_target'):
        assert before[key] == after[key]
    assert before['actual'] != after['actual']


def test_publication_lags_and_age_limits_preserve_original_observation_age():
    start = date(2020, 1, 1)
    rows = {start: (100.0, 10.0, 1.0, 0.5), start+timedelta(days=1): (None,)*4}
    calendar = M['available_calendar'](rows, start+timedelta(days=15), (1, 3, 3, 3))
    assert calendar[start] == [None]*4
    assert calendar[start+timedelta(days=1)][0] == 100
    assert calendar[start+timedelta(days=2)][1] is None
    assert calendar[start+timedelta(days=3)][1] == 10
    assert calendar[start+timedelta(days=7)][0] == 100
    assert calendar[start+timedelta(days=8)][0] is None
    assert calendar[start+timedelta(days=15)][1] is None


def test_each_origin_refits_a_new_model_and_uses_matched_baseline_samples():
    fits = []

    def factory():
        value = Model()
        fits.append(value)
        return value

    result = M['simulate'](observations(), date(2020, 9, 26), date(2020, 1, 6), date(2020, 3, 6),
                           minimum_training=30, model_factory=factory)
    assert len(fits) == len(result['predictions']) == 3
    assert len({id(model) for model in fits}) == 3
    assert {score['n'] for score in result['metrics'].values()} == {3}


def test_unquoted_targets_are_counted_as_skipped_not_scored_with_filled_prices():
    rows = observations()
    rows[date(2020, 2, 15)] = (None, 1.0, 1.0, 0.5)
    report = evaluate(rows)
    assert report['predictions'] == []
    assert report['skipped'][0]['reason'] == 'no_observed_target'
    assert all(metric['n'] == 0 and metric['mae'] is None for metric in report['metrics'].values())


@pytest.mark.parametrize('lags', [(8, 3, 3, 3), (-1, 3, 3, 3), ()])
def test_invalid_publication_assumptions_are_rejected(lags):
    with pytest.raises(ValueError):
        evaluate(observations(), lags=lags)


def test_evaluation_period_cannot_include_immature_targets():
    with pytest.raises(ValueError, match='immature'):
        M['simulate'](observations(), date(2020, 2, 15), date(2020, 1, 6), date(2020, 1, 6))


@pytest.mark.parametrize('lines', [
    '2020-01-01,100,10,1,0.5\n2020-01-01,101,10,1,0.5\n',
    '2030-01-01,100,10,1,0.5\n', '2020-01-01,NaN,10,1,0.5\n',
])
def test_invalid_snapshot_data_is_rejected(tmp_path, lines):
    path = tmp_path/'input.csv'
    path.write_text('date,cocoa_price,oil_price,precipitation,soil_moisture\n'+lines)
    with pytest.raises(ValueError):
        M['read_inputs'](path, date(2020, 12, 31))


def test_metrics_have_defined_units_and_no_zero_sample_accuracy_claim():
    result = M['score']([100,200],[110,180])
    assert result['mae'] == 15 and result['rmse'] == math.sqrt(250)
    assert result['mape_percent'] == 10
    assert M['score']([],[]) == {'n': 0, 'mae': None, 'rmse': None, 'mape_percent': None}


def test_missing_sentinels_do_not_become_weather_predictors(tmp_path):
    path = tmp_path/'input.csv'
    path.write_text('date,cocoa_price,oil_price,precipitation,soil_moisture\n2020-01-01,100,10,-999,-999\n')
    assert M['read_inputs'](path, date(2020, 1, 1))[date(2020, 1, 1)] == (100,10,None,None)


def test_invalid_prices_are_flagged_and_their_errors_are_not_hidden():
    class Negative(Model):
        def predict(self, x):
            return [-100]

    result = M['simulate'](observations(),date(2020,9,26),date(2020,1,6),date(2020,1,6),
                           minimum_training=30,model_factory=Negative)
    assert result['status'] == 'invalid_candidate_predictions'
    assert result['invalid_forecast_count'] == 1 and result['metrics']['local_ridge']['n'] == 1
    assert result['metrics']['local_ridge']['mae'] > result['predictions'][0]['actual']
