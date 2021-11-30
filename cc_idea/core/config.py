from pathlib import Path


class Paths:
    def __init__(self):
        self.repo = Path(__file__).absolute().parents[2]
        self.data = self.repo / 'data'
        self.tests = self.repo / 'tests'


paths = Paths()
