from json.encoder import (
    _make_iterencode,
    JSONEncoder,
    encode_basestring_ascii,
    INFINITY,
    c_make_encoder,
    encode_basestring,
)


class CustomObjectEncoder(JSONEncoder):
    def should_use_original(self, o, cls) -> bool:
        return False

    def iterencode(self, o, _one_shot=False):
        """
        This is based on:
        https://stackoverflow.com/questions/16405969/how-to-change-json-encoding-behaviour-for-serializable-python-object?newreg=14b801f45f1d41699a2ffe0f8a4ee711

        I copied this function from json.JSONEncoder.iterencode, and changed:
        (1) _one_shot is always False
        (2) use our own function to decide which types we want to handle ourselves: should_use_original.
        """
        # Force the use of _make_iterencode instead of c_make_encoder
        _one_shot = False

        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = encode_basestring_ascii
        else:
            _encoder = encode_basestring

        def floatstr(
            o, allow_nan=self.allow_nan, _repr=float.__repr__, _inf=INFINITY, _neginf=-INFINITY
        ):

            if o != o:
                text = "NaN"
            elif o == _inf:
                text = "Infinity"
            elif o == _neginf:
                text = "-Infinity"
            else:
                return _repr(o)

            if not allow_nan:
                raise ValueError("Out of range float values are not JSON compliant: " + repr(o))

            return text

        if _one_shot and c_make_encoder is not None and self.indent is None and not self.sort_keys:
            _iterencode = c_make_encoder(
                markers,
                self.default,
                _encoder,
                self.indent,
                self.key_separator,
                self.item_separator,
                self.sort_keys,
                self.skipkeys,
                self.allow_nan,
            )
        else:
            _iterencode = _make_iterencode(
                markers,
                self.default,
                _encoder,
                self.indent,
                floatstr,
                self.key_separator,
                self.item_separator,
                self.sort_keys,
                self.skipkeys,
                _one_shot,
                isinstance=self.should_use_original,
            )  # this is the only changed line!
        return _iterencode(o, 0)
