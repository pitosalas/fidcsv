import pytest
from pathlib import Path
from box import Box

def test_config_loads():
    config_path = Path(__file__).parent.parent / "fidcsv" / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)

    assert config.data is not None
    assert config.output is not None
    assert config.charapi_config_path is not None
    assert config.fields is not None

def test_config_has_required_fields():
    config_path = Path(__file__).parent.parent / "fidcsv" / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)

    assert "ein" in config.fields
    assert "organization_name" in config.fields
    assert "mission" in config.fields

def test_all_fields_have_include():
    config_path = Path(__file__).parent.parent / "fidcsv" / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)

    for field_name, field_config in config.fields.items():
        assert hasattr(field_config, "include")
        assert isinstance(field_config.include, bool)
