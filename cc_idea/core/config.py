import yaml
from pathlib import Path
from typing import Dict
from cc_idea.core.symbol import Symbol



class Paths:
    def __init__(self):
        self.repo: Path = Path(__file__).absolute().parents[2]
        self.package: Path = self.repo / 'cc_idea'
        self.reports: Path = self.package / 'reports'
        self.data: Path = self.repo / 'data'
        self.tests: Path = self.repo / 'tests'


def _get_config() -> dict:
    with open(Path(__file__).parent / 'config.yaml', 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def _get_symbols() -> Dict[str, Symbol]:
    with open(Path(__file__).parent / 'symbols.yaml', 'r') as file:
        data = yaml.load(file, Loader=yaml.FullLoader)
        return {key: Symbol(symbol=key, **value) for key, value in data.items()}



paths = Paths()
config = _get_config()
symbols = _get_symbols()
