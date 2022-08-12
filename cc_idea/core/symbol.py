from typing import List



class Symbol:
    """TODO:  Document."""

    def __init__(self, **args):
        self.id: str = args['symbol']
        self.name: str = args['name']
        self.yahoo_symbol: str = args['yahoo_symbol']
        self.reddit_q: List[str] = self._to_list(args.get('reddit_q'))
        self.subreddits: List[str] = self._to_list(args.get('subreddits'))

    def __repr__(self):
        return self.id

    def _to_list(self, x: str) -> List[str]:
        if isinstance(x, str):
            return [str(y).strip() for y in x.split(',')]
        else:
            return []
