#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_state import HDXState
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.unosat._version import __version__
from hdx.scraper.unosat.pipeline import Pipeline
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unosat"
updated_by_script = "HDX Scraper: UNOSAT"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "ba5aacba-0633-4364-9528-bc76a3f6cf95", configuration=configuration
    )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with HDXState(
            "pipeline-state-unosat",
            folder,
            parse_date,
            iso_string_from_datetime,
            configuration,
        ) as state:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                pipeline = Pipeline(configuration, retriever)
                last_build_date, entries = pipeline.parse_feed(state.get())
                logger.info(f"Number of datasets: {len(entries)}")

                for _, entry in progress_storing_folder(info, entries, "title"):
                    dataset, showcase = pipeline.generate_dataset(entry)
                    if not dataset:
                        continue
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    # ensure markdown has line breaks
                    dataset["notes"] = dataset["notes"].replace("\n", "  \n")

                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=info["batch"],
                    )
                    if not showcase:
                        continue
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)
                state.set(last_build_date)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
