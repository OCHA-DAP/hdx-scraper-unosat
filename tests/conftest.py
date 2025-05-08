from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.unosat.__main__ import main
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def configuration():
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
    Locations.set_validlocations(
        [
            {"name": "vut", "title": "vut"},
            {"name": "ssd", "title": "ssd"},
            {"name": "pak", "title": "pak"},
            {"name": "irq", "title": "irq"},
        ]
    )
    configuration = Configuration.read()
    tags = configuration["tag_mapping"].values()
    Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
    tags = [{"name": "geodata"}] + [{"name": tag} for tag in tags]
    Vocabulary._approved_vocabulary = {
        "tags": tags,
        "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
        "name": "approved",
    }
    return configuration
