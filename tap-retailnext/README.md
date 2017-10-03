# tap-retailnext

singer.io tap- extracting data from the RetailNext REST API, written in python 3.5.

Author: Ashwani Singh (ashwani.s@blueoceanmi.com)



1. Install

    >python setup.py install 
    >tap-retailnext -h

2. Execution and configuration options:

    tap-retailnext takes two inputs arguments
     I. --config:  It takes a directory as input where all the conf files are located.

Note: Struture should be maintained as it is and refer folder tap_retailnext for file structure.

     II. --loadtype: Currently metrics are  aggregated at minute or day level however some metrics are not groupable at minute or hour level from RetailNext API so, those are kept at day level. Metrices has been seprated and it is kept in tap_retailnext/filters/[day_metrics \metrics].json. Use minute option for minute level data extract and day option for day level.

Note: For minute level data extraction, the time for incremental load is after every 15 mins and can be updated as per requirement in date_filter variable under function headers_min. For day level extraction, it will be next day and can be updated through next_day variable under function headers_day.

     As metrices are grouped on different level filters, options are also different and can be found in directory ap_retailnext/filters/[day_filter\filters].json.

Note: State will be written automatically in filter.json and day_filter.json file and next time it will automatically pick filter files and further run it.


3. Running the application:
    > tap-retailnext  --config folderPath  --loadtype  day\minute

