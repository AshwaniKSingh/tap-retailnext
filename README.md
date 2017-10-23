# tap-retailnext

singer.io tap- Extracts data from the RetailNext REST API, written in python 3.5.

Author: Ashwani Singh (ashwani.s@blueoceanmi.com)



1. Install

    >python setup.py install 
    >tap-retailnext -h

2. Execution and configuration options:

    tap-retailnext takes two inputs arguments
     
     I. --config:  It takes a configuration file as authentication parameters and parameters are "Access Key", "Secret Key" and "user_agent" as well as some additional parameter like start_date(when first time time configured this parameter will be used as start date for date extraction ), "increment" parameter, this parameter will be used to identify end date for data extraction filter and "type" will be used as day or minute grouping option for respective metrics.

     II. --state: It is an optional parameter, this file defines till what date or time data has been extracted. Format of this file is same as filter format which RetailNext supports, for more information refer http://docs.retailnext.net/cloud/api/.

Note: For incremental options please update "increment" parameter in config file, this will be based on "type" parameter.

    
3. Running the application:
    > tap-retailnext  --config config.json  [--state  state.json]

