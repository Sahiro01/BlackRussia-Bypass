class EventBus:

    def __init__(self):
        self._handlers = {}

    def on(self, name, fn):

        self._handlers.setdefault(str(name), []).append(fn)
        return fn

    def handler(self, name):

        def deco(fn):
            self.on(name, fn)
            return fn
        return deco

    def emit(self, name, *args):

        for fn in list(self._handlers.get(str(name), ())):
            try:
                if fn(*args):
                    return True
            except Exception as exc:
                from core import logger
                logger.warn(f'script handler {name} failed: {exc}')
        return False
