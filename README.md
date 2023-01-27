### Collector for UNOSAT's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-unosat/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-unosat/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-unosat/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-unosat?branch=main)

This script connects to the [UNOSAT API](https://unosat.org/product/feed/) and extracts data creating a dataset for each item in teh feed in HDX. It makes 1 read to UNOSAT's feed and 100 read/writes (API calls) to HDX in a half hour period. It is run every week.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-unosat** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY
