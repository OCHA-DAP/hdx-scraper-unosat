#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from unosat import UNOSAT

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

    configuration = Configuration.read()
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            today = now_utc()
            unosat = UNOSAT(configuration, retriever, today)
            entries = unosat.parse_feed()
            logger.info(f"Number of datasets: {len(entries)}")

            for _, entry in progress_storing_folder(info, entries, "title"):
                dataset, showcase = unosat.generate_dataset(entry)
                if not dataset:
                    continue
                dataset.update_from_yaml(join("config", "hdx_dataset_static.yml"))
                # ensure markdown has line breaks
                dataset["notes"] = dataset["notes"].replace("\n", "  \n")

                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                    batch=info["batch"],
                )
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
