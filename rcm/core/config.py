import multiprocessing as mp
import yaml
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List
from rcm.core.symbol import Symbol



class Paths:
    """Common file paths."""
    def __init__(self):
        self.repo: Path = Path(__file__).absolute().parents[2]
        self.package: Path = self.repo / 'rcm'
        self.reports: Path = self.package / 'reports'
        self.data: Path = self.repo / 'data'
        self.tests: Path = self.repo / 'tests'



class Config:
    """Reads application config files into memory."""

    def __init__(self):
        self._yaml: Dict = self._get_yaml()
        self.symbols: Dict[str, Symbol] = self._get_symbols()
        self.extractors: ExtractorConfig = ExtractorConfig(self)
        self.transformers: TransformerConfig = TransformerConfig(self)

    def _get_yaml(self) -> Dict:
        with open(paths.package / 'core' / 'config.yaml', 'r') as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    def _get_symbols(self) -> Dict[str, Symbol]:
        with open(paths.package / 'core' / 'symbols.yaml', 'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
            return {key: Symbol(symbol=key, **value) for key, value in data.items()}



class ExtractorConfig:

    def __init__(self, config: Config):
        self.yahoo: YahooExtractorConfig = YahooExtractorConfig(config)
        self.reddit: RedditExtractorConfig = RedditExtractorConfig(config)



class YahooExtractorConfig:

    def __init__(self, config: Config):
        self.symbols: List[str] = self._get_symbols(config)

    def _get_symbols(self, config: Config) -> List[str]:
        """Parses config file and returns list of in-scope Yahoo Finance symbols."""
        symbols = config._yaml['extractors']['yahoo']['queries'][0]['symbols']
        if symbols == 'all':
            return [x.yahoo_symbol for x in config.symbols.values()]
        else:
            return symbols



class RedditExtractorConfig:

    def __init__(self, config: Config):
        self.min_date: date = datetime.strptime(config._yaml['extractors']['reddit']['min_date'], '%Y-%m-%d').date()
        self.max_date: date = datetime.strptime(config._yaml['extractors']['reddit']['max_date'], '%Y-%m-%d').date()
        self.queries: List[Dict] = self._get_queries(config)

    def _get_queries(self, config: Config) -> List[Dict]:
        """Parses config file and returns list of in-scope Pushshift API queries."""
        queries = []
        for query in config._yaml['extractors']['reddit']['queries']:
            if 'words' in query:
                search_type = 'word'
                search_values = query['words'] if query['words'] != 'all' else [x for _, symbol in config.symbols.items() for x in symbol.words]
            if 'subreddits' in query:
                search_type = 'subreddit'
                search_values = query['subreddits'] if query['subreddits'] != 'all' else [x for _, symbol in config.symbols.items() for x in symbol.subreddits]
            for v in sorted(list(set(search_values))):
                queries.append({
                    'endpoint': query['endpoint'],
                    'search': (search_type, v),
                    'min_score': query['min_score'],
                    'min_date': self.min_date,
                    'max_date': self.max_date,
                })
        return queries



class TransformerConfig:

    def __init__(self, config: Config):
        self.sentiment: SentimentTransformerConfig = SentimentTransformerConfig(config)



class SentimentTransformerConfig:

    def __init__(self, config: Config):
        self.chunk_size: int = config._yaml['transformers']['sentiment']['chunk_size']
        self.processes: int = self._get_processes(config)

    def _get_processes(self, config: Config) -> int:
        processes = config._yaml['transformers']['sentiment']['processes']
        if processes == 'auto':
            return mp.cpu_count()
        else:
            return processes


paths = Paths()
config = Config()
