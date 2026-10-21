"""Package that re-exports its executor lazily, like disslucc.executors."""


def __getattr__(name):
    if name == "LazyExecutor":
        from .impl import LazyExecutor
        return LazyExecutor
    raise AttributeError(name)
