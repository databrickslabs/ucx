import enum


class StrEnum(str, enum.Enum):  # re-exported for compatability with older python versions
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, str | enum.auto):
            msg = f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            raise TypeError(msg)
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):  # noqa: N805
        return name
