from datetime import datetime
from cc_idea.extractors.reddit import _load_comments



def test_load_comments():
    time_intervals = [
        ('[A, B]', 'cardano', datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1,  8, 0, 0)),
        ('[B, C]', 'cardano', datetime(2021, 1, 1, 8, 0, 0), datetime(2021, 1, 1, 16, 0, 0)),
        ('[A, C]', 'cardano', datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1, 16, 0, 0)),
    ]
    results = {
        name: _load_comments(q, start_date, end_date)
        for name, q, start_date, end_date in time_intervals
    }
    assert len(results['[A, C]']) == len(results['[A, B]']) + len(results['[B, C]'])
