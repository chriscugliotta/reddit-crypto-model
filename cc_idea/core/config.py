from pathlib import Path


class Paths:
    def __init__(self):
        self.repo: Path = Path(__file__).absolute().parents[2]
        self.package: Path = self.repo / 'cc_idea'
        self.reports: Path = self.package / 'reports'
        self.data: Path = self.repo / 'data'
        self.tests: Path = self.repo / 'tests'


paths = Paths()
