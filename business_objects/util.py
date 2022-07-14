def set_values_on_item(item, **kwargs):
    for key in kwargs.keys():
        if not hasattr(item, key):
            raise KeyError(f"Setting value {kwargs[key]} to key {key} on Item failed.")
        else:
            setattr(item, key, kwargs[key])
    return item
