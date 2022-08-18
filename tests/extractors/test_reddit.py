from datetime import datetime
from cc_idea.extractors.reddit import RedditExtractor



def test_reddit_extractor():
    """Verify that Pushshift API time filters are well-behaved, e.g. [A, C] = [A, B] + [B, C]."""
    time_intervals = [
        ('[A, B]', 'cardano', datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1,  8, 0, 0)),
        ('[B, C]', 'cardano', datetime(2021, 1, 1, 8, 0, 0), datetime(2021, 1, 1, 16, 0, 0)),
        ('[A, C]', 'cardano', datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1, 16, 0, 0)),
    ]
    results = {
        name: RedditExtractor()._extract_api('comment', {'word': word}, min_date, max_date)
        for name, word, min_date, max_date in time_intervals
    }
    assert len(results['[A, C]']) == len(results['[A, B]']) + len(results['[B, C]'])
