#!/usr/bin/python
"""
UNOSAT:
----

Reads UNOSAT data and creates datasets.

"""

import logging

import feedparser
from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_filename_from_url

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration, retriever):
        self.configuration = configuration
        self.retriever = retriever
        self.last_build_date = None

    def parse_feed(self, previous_build_date):
        url = self.configuration["url"]
        rssfile = self.retriever.download_file(url, keep=True)
        feed = feedparser.parse(rssfile)
        last_build_date = parse_date(feed.feed.updated)
        results = []
        if last_build_date <= previous_build_date:
            return previous_build_date, results
        for entry in feed.entries:
            published = parse_date(entry.published)
            if published > previous_build_date:
                entry.published = published
                results.append(entry)
        return last_build_date, results

    def generate_dataset(
        self,
        entry,
    ):
        """ """
        title = entry.title
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(title)
        if len(slugified_name) > 90:
            slugified_name = slugified_name.replace("satellite-detected-", "")
            slugified_name = slugified_name.replace("estimation-of-", "")
            slugified_name = slugified_name.replace("geodata-of-", "")[:90]
        event_code = entry.eventcode
        gdacs_eventid = entry.gdacs_eventid
        if gdacs_eventid and gdacs_eventid.lower() != "none":
            gdacs_id = f", GDACS ID: {gdacs_eventid}"
        else:
            gdacs_id = ""
        summary = entry.summary
        notes = f"**UNOSAT code: {event_code}{gdacs_id}**  {summary}"
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": notes,
            }
        )
        dataset.set_maintainer("83fa9515-3ba4-4f1d-9860-f38b20f80442")
        dataset.set_organization("ba5aacba-0633-4364-9528-bc76a3f6cf95")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        countryiso3 = entry.iso3
        if not countryiso3:
            logger.error(f"ISO3 is blank for {title}!")
            return None, None
        for countryiso in entry.iso3.split(";"):
            dataset.add_country_location(countryiso)
        tag_mapping = self.configuration["tag_mapping"]
        tags = ["geodata"]
        for tag in entry.tags:
            tag = tag_mapping.get(tag.term)
            if tag:
                tags.append(tag)
        dataset.add_tags(tags)
        dataset.set_time_period(entry.published)

        def get_resource(link, file_format, description):
            filename = get_filename_from_url(link)
            resource = Resource(
                {
                    "name": filename,
                    "format": file_format,
                    "url": link,
                    "description": description,
                }
            )
            resource.set_date_data_updated(entry.published)
            return resource

        resources = []
        gdb_link = entry.gdb_link
        if gdb_link:
            resources.append(
                get_resource(gdb_link, "Geodatabase", "Zipped geodatabase")
            )
        shp_link = entry.shp_link
        if shp_link:
            resources.append(get_resource(shp_link, "SHP", "Zipped shapefile"))
        kml_link = entry.kml_link
        if kml_link:
            resources.append(get_resource(kml_link, "KML", "KML file"))
        excel_link = entry.excel
        if excel_link:
            resources.append(get_resource(excel_link, "XLSX", "Excel file"))

        if not resources:
            logger.error(f"Dataset {title} has no resources!")
            return None, None
        dataset.add_update_resources(resources)

        showcase_link = entry.wmap_link
        title = "WMap Link"
        if not showcase_link:
            showcase_link = entry.pdf
            title = "Static PDF Map"
        if not showcase_link:
            return dataset, None
        image_link = None
        for link in entry.links:
            if "image" in link.type:
                image_link = link.href
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": title,
                "notes": "Click to go to showcase",
                "url": showcase_link,
                "image_url": image_link,
            }
        )
        showcase.add_tags(tags)

        return dataset, showcase
