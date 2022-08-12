import pandas as pd
import yaml
from pathlib import Path
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


def _get_symbols() -> dict:
    df = pd.read_excel(Path(__file__).parent / 'symbols.xlsx')
    return [Symbol(**row) for row in df.to_dict(orient='records')]


paths = Paths()
config = _get_config()
symbols = _get_symbols()
