#!/usr/bin/python
"""
Unit tests for UNOSAT.

"""

from datetime import datetime, timezone
from os.path import join

import pytest

from hdx.scraper.unosat.pipeline import Pipeline
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve


class TestUNOSAT:
    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    def test_generate_datasets_and_showcases(self, configuration, fixtures):
        with temp_dir(
            "test_unosat", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                pipeline = Pipeline(configuration, retriever)
                last_build_date, entries = pipeline.parse_feed(
                    datetime(2020, 2, 9, 0, 0, tzinfo=timezone.utc)
                )
                assert last_build_date == datetime(
                    2023, 1, 25, 16, 5, 21, tzinfo=timezone.utc
                )
                assert len(entries) == 3

                dataset, showcase = pipeline.generate_dataset(entries[0])
                assert dataset is None

                dataset, showcase = pipeline.generate_dataset(entries[1])
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-01-25T00:00:00 TO 2023-01-25T00:00:00]",
                    "groups": [{"name": "ssd"}],
                    "maintainer": "83fa9515-3ba4-4f1d-9860-f38b20f80442",
                    "name": "satellite-detected-water-extents-between-17-and-21-january-2023-over-south-sudan",
                    "notes": "**UNOSAT code: FL20220424SSD**  This map illustrates "
                    "cumulative satellite-detected water using VIIRS in South Sudan "
                    "between 17 to 21 January 2023 compared with the period from 12 to "
                    "16 January 2023. Within the cloud free analyzed areas of about "
                    "629,000 km², a total of about 47,700 km² of lands appear to be "
                    "affected with flood waters. Water extent appears to have increased "
                    "of about 3,000 km² since the period between 12 to 16 January 2022. "
                    "Based on Worldpop population data and the maximal flood water "
                    "extent ~795,000 people are potentially exposed or living close to "
                    "flooded areas.\n"
                    "This is a preliminary analysis and has not yet been validated in "
                    "the field. Please send ground feedback to the United Nations "
                    "Satellite Centre (UNOSAT).",
                    "owner_org": "ba5aacba-0633-4364-9528-bc76a3f6cf95",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "flooding-storm surge",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Satellite detected water extents between 17 and 21 January 2023 "
                    "over South Sudan",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Zipped geodatabase",
                        "format": "Geodatabase",
                        "last_modified": "2023-01-25T14:07:32.000000",
                        "name": "FL20220424SSD.gdb.zip",
                        "url": "https://unosat.org/static/unosat_filesystem/3473/FL20220424SSD.gdb.zip",
                    },
                    {
                        "description": "Zipped shapefile",
                        "format": "SHP",
                        "last_modified": "2023-01-25T14:07:32.000000",
                        "name": "FL20220424SSD_SHP.zip",
                        "url": "https://unosat.org/static/unosat_filesystem/3473/FL20220424SSD_SHP.zip",
                    },
                    {
                        "description": "Excel file",
                        "format": "XLSX",
                        "last_modified": "2023-01-25T14:07:32.000000",
                        "name": "UNOSAT_Population_Exposure_FL20220424SSD_17Jan_21Jan2023_SouthSudan_Week9.xlsx",
                        "url": "https://unosat.org/static/unosat_filesystem/3473/UNOSAT_Population_Exposure_FL20220424SSD_17Jan_21Jan2023_SouthSudan_Week9.xlsx",
                    },
                ]
                assert showcase == {
                    "image_url": "https://unosat.org/static/unosat_filesystem/3473/UNOSAT_A3_Natural_Landscape_FL20220424SSD_17Jan_21Jan2023_SouthSudan_Week9.JPG",
                    "name": "satellite-detected-water-extents-between-17-and-21-january-2023-over-south-sudan-showcase",
                    "notes": "Click to go to showcase",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "flooding-storm surge",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "WMap Link",
                    "url": "https://unosat.maps.arcgis.com/apps/dashboards/b3c40fc3e6ec46668b26019db0b11f7c",
                }

                dataset, showcase = pipeline.generate_dataset(entries[2])
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-01-20T00:00:00 TO 2023-01-20T00:00:00]",
                    "groups": [{"name": "pak"}],
                    "maintainer": "83fa9515-3ba4-4f1d-9860-f38b20f80442",
                    "name": "preliminary-satellite-derived-flood-evolution-assessment-islamic-republic-of-pakistan-20-j",
                    "notes": "**UNOSAT code: FL20221121PAK**  Status: Overall "
                    "decrease of flood waters observed\n"
                    "Further action(s): continue monitoring\n"
                    "Evolution of Cumulative Flood Waters over I.R. of Pakistan (01-07 "
                    "January 2023 Vs 09-15 January 2023):\n"
                    "Between 09 and 15 January 2023 approximately 4.5 million people "
                    "remain potentially exposed or living close to maximum floodwaters "
                    "areas(*);\n"
                    "Approximately 1.3 million people are potentially exposed or living "
                    "close to minimum floodwaters areas(**) between 09 and 15 January "
                    "2023;\n"
                    "Based on satellite observations between 01 and 07 January 2023 and "
                    "compared with observations between 09 and 15 January 2023 , the "
                    "maximum flood water extent appears to continue to retract with "
                    "approximately ~ -1,800 km² in Sindh , ~ - 250 km² in Balochistan, ~ "
                    "-200 km²  in Khyber Pakhtunkhwa and ~ -200 km²  in Gilgit "
                    "Baltistan.",
                    "owner_org": "ba5aacba-0633-4364-9528-bc76a3f6cf95",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "flooding-storm surge",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Preliminary Satellite Derived Flood Evolution Assessment, Islamic "
                    "Republic of Pakistan - 20 January 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Zipped geodatabase",
                        "format": "Geodatabase",
                        "last_modified": "2023-01-20T14:44:27.000000",
                        "name": "FL20221121PAK_gdb.zip",
                        "url": "https://unosat.org/static/unosat_filesystem/3471/FL20221121PAK_gdb.zip",
                    },
                    {
                        "description": "Zipped shapefile",
                        "format": "SHP",
                        "last_modified": "2023-01-20T14:44:27.000000",
                        "name": "FL20221121PAK_SHP.zip",
                        "url": "https://unosat.org/static/unosat_filesystem/3471/FL20221121PAK_SHP.zip",
                    },
                    {
                        "description": "Excel file",
                        "format": "XLSX",
                        "last_modified": "2023-01-20T14:44:27.000000",
                        "name": "UNOSAT_Population_Exposure_FL20221121PAK_WeeklyUpdate_09Jan2023_15Jan2023_Pakistan.xlsx",
                        "url": "https://unosat.org/static/unosat_filesystem/3471/UNOSAT_Population_Exposure_FL20221121PAK_WeeklyUpdate_09Jan2023_15Jan2023_Pakistan.xlsx",
                    },
                ]
                assert showcase == {
                    "image_url": "https://unosat.org/static/unosat_filesystem/3471/UNOSAT_Preliminary_Assessment_Report_FL20221121PAK_Pakistan_WeeklyUpdate_20230120.jpg",
                    "name": "preliminary-satellite-derived-flood-evolution-assessment-islamic-republic-of-pakistan-20-j-showcase",
                    "notes": "Click to go to showcase",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "flooding-storm surge",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Static PDF Map",
                    "url": "https://unosat.org/static/unosat_filesystem/3471/UNOSAT_Preliminary_Assessment_Report_FL20221121PAK_Pakistan_WeeklyUpdate_20230120.pdf",
                }
