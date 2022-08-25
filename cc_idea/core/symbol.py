from typing import List



class Symbol:
    """Contains all metadata related to a single stock or coin symbol."""

    def __init__(self, **args):
        self.id: str = args['symbol']
        self.name: str = args['name']
        self.yahoo_symbol: str = args['yahoo_symbol']
        self.words: List[str] = self._to_list(args.get('words'))
        self.subreddits: List[str] = self._to_list(args.get('subreddits'))

    def __repr__(self):
        return self.id

    def _to_list(self, x: str) -> List[str]:
        if isinstance(x, str):
            return [str(y).strip() for y in x.split(',')]
        else:
            return []
