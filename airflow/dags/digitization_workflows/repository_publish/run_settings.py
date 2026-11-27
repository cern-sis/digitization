"""The settings of a run, which depend on the environment (local, QA, production).

Each setting comes from, in order:

1. the trigger param, if it is set;
2. the JSON Variable `publish_to_repository` of the environment;
3. the default below. `bucket`, `prefix` and `community` have no default: a run
   without them fails, instead of writing to the wrong place.
"""

from digitization_workflows.repository_publish import config

REQUIRED = ("bucket", "prefix", "community")
DEFAULTS = {
    "extension": ".pdf",
    "resource_type": config.DEFAULT_RESOURCE_TYPE,
    "done_folder": config.DEFAULT_DONE_FOLDER,
    "max_deposits": config.DEFAULT_MAX_DEPOSITS,
    "reports_folder": config.DEFAULT_REPORTS_FOLDER,
}
KEYS = REQUIRED + tuple(DEFAULTS)
INTEGER_KEYS = {"max_deposits"}


class SettingsError(ValueError):
    pass


def resolve(params: dict, environment: object) -> dict:
    """The settings of the run, from the trigger params and the Variable content."""
    environment = _checked_environment(environment)
    settings = {}
    for key in KEYS:
        value = _first_set(params.get(key), environment.get(key), DEFAULTS.get(key))
        if value is None:
            raise SettingsError(
                f"Set '{key}' in the '{config.SETTINGS_VARIABLE}' Variable "
                "or in the trigger params."
            )
        settings[key] = _checked(key, value)
    return settings


def defaults(environment: object) -> dict:
    """The values shown in the trigger form: the Variable, else the defaults.

    The settings without a default are None when the Variable does not set them.
    """
    environment = _checked_environment(environment)
    return {
        key: _checked(key, environment[key])
        if environment.get(key) is not None
        else DEFAULTS.get(key)
        for key in KEYS
    }


def _checked_environment(environment: object) -> dict:
    if environment is None:
        return {}
    if not isinstance(environment, dict):
        raise SettingsError(
            f"The '{config.SETTINGS_VARIABLE}' Variable must be a JSON object."
        )
    unknown = sorted(set(environment) - set(KEYS))
    if unknown:
        raise SettingsError(
            f"Unknown keys in the '{config.SETTINGS_VARIABLE}' Variable: {unknown}. "
            f"Allowed: {list(KEYS)}."
        )
    return environment


def _first_set(*values):
    return next((value for value in values if value is not None), None)


def _checked(key: str, value: object) -> object:
    if key in INTEGER_KEYS:
        # bool is a subclass of int: refuse `true` as well.
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise SettingsError(f"'{key}' must be an integer >= 1, not {value!r}.")
    elif not isinstance(value, str):
        raise SettingsError(f"'{key}' must be a string, not {value!r}.")
    return value
