def _exception_from_packed_args(exception_cls, args=None, kwargs=None):
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    return exception_cls(*args, **kwargs)


class BaseError(Exception):
    """The base exception class for errors."""

    fmt = "An unspecified error occurred"

    def __init__(self, *args, **kwargs):
        msg = self.fmt.format(*self.args, **kwargs)
        Exception.__init__(self, msg)
        self.args = args
        self.kwargs = kwargs

    def __reduce__(self):
        return _exception_from_packed_args, (
            self.__class__,
            self.args,
            self.kwargs,
        )


class MultipleObjectsReturned(BaseError):
    fmt = "MultipleObjectsReturned: {n} items returned, instead of 1."
