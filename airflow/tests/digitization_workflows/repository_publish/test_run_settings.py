import pytest

from digitization_workflows.repository_publish import run_settings
from digitization_workflows.repository_publish.run_settings import SettingsError

ENVIRONMENT = {
    "bucket": "cern-archives-multilateralism",
    "prefix": "atom test",
    "community": "multilateralism",
}


def test_the_variable_gives_the_environment_settings():
    settings = run_settings.resolve({}, ENVIRONMENT)

    assert settings == {
        "bucket": "cern-archives-multilateralism",
        "prefix": "atom test",
        "community": "multilateralism",
        "extension": ".pdf",
        "resource_type": "publication",
        "done_folder": "migrated",
        "max_deposits": 50,
        "reports_folder": "reports",
    }


def test_trigger_params_override_the_variable():
    params = {"prefix": "other folder", "max_deposits": 1, "bucket": None}

    settings = run_settings.resolve(params, ENVIRONMENT)

    assert settings["prefix"] == "other folder"
    assert settings["max_deposits"] == 1
    assert settings["bucket"] == "cern-archives-multilateralism"


def test_the_variable_overrides_the_defaults():
    environment = {**ENVIRONMENT, "extension": ".pdf,.jpg", "max_deposits": 10}

    settings = run_settings.resolve({}, environment)

    assert settings["extension"] == ".pdf,.jpg"
    assert settings["max_deposits"] == 10


def test_an_empty_done_folder_is_kept():
    """An empty done folder means: do not move the files."""
    settings = run_settings.resolve({"done_folder": ""}, ENVIRONMENT)

    assert settings["done_folder"] == ""


@pytest.mark.parametrize("missing", ["bucket", "prefix", "community"])
def test_settings_without_a_default_are_required(missing):
    environment = {k: v for k, v in ENVIRONMENT.items() if k != missing}

    with pytest.raises(SettingsError, match=f"'{missing}'.*publish_to_repository"):
        run_settings.resolve({}, environment)


def test_no_variable_and_no_params():
    with pytest.raises(SettingsError, match="'bucket'"):
        run_settings.resolve({}, None)


def test_unknown_keys_in_the_variable_are_refused():
    """A typo in the Variable must not be ignored silently."""
    with pytest.raises(SettingsError, match="comunity"):
        run_settings.resolve({}, {**ENVIRONMENT, "comunity": "x"})


@pytest.mark.parametrize(
    "environment",
    [
        {**ENVIRONMENT, "max_deposits": "10"},
        {**ENVIRONMENT, "max_deposits": 0},
        {**ENVIRONMENT, "max_deposits": True},
        {**ENVIRONMENT, "bucket": 12},
    ],
)
def test_wrong_types_are_refused(environment):
    with pytest.raises(SettingsError):
        run_settings.resolve({}, environment)


def test_the_variable_must_be_a_json_object():
    with pytest.raises(SettingsError, match="JSON object"):
        run_settings.resolve({}, ["bucket"])


def test_form_defaults_show_the_variable_then_the_defaults():
    defaults = run_settings.defaults({"bucket": "b", "done_folder": ""})

    assert defaults == {
        "bucket": "b",
        "prefix": None,
        "community": None,
        "extension": ".pdf",
        "resource_type": "publication",
        "done_folder": "",
        "max_deposits": 50,
        "reports_folder": "reports",
    }


def test_form_defaults_without_a_variable():
    assert run_settings.defaults(None)["bucket"] is None
    assert run_settings.defaults(None)["max_deposits"] == 50


def test_form_defaults_check_the_variable():
    with pytest.raises(SettingsError):
        run_settings.defaults({"comunity": "x"})
