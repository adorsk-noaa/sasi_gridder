class Cell(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class StatArea(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Effort(object):
    # Attrs which represent values.
    value_attrs = ['a', 'hours_fished', 'value']

    # Attrs used for grouping efforts.
    key_attrs = ['gear_id']

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
