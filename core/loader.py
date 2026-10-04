import importlib.util
import pathlib

from core import logger

_SKIP = frozenset({'__init__.py'})


def load_scripts(api, bus, dirname=None):
    root = pathlib.Path(dirname) if dirname else pathlib.Path(__file__).parent.parent / 'scripts'
    loaded = []

    if not root.is_dir():
        return loaded

    for path in sorted(root.glob('*.py')):
        if path.name.startswith(('_', '.')) or path.name in _SKIP:
            continue
        try:
            spec = importlib.util.spec_from_file_location(
                f'scripts.{path.stem}', path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            setup = getattr(mod, 'setup', None)
            if callable(setup):
                setup(api, bus)
            loaded.append(path.stem)
        except Exception as exc:
            logger.warn(f'script {path.name} failed: {exc}')

    if loaded:
        logger.info(f'scripts loaded: {", ".join(loaded)}')

    return loaded
