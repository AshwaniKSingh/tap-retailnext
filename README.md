# tap-retailnext

singer.io tap- Extracts data from the RetailNext REST API, written in python 3.5.

Author: Ashwani Singh (ashwani.s@blueoceanmi.com)



1. Install

    >python setup.py install 
    >tap-retailnext -h

2. Execution and configuration options:

    tap-retailnext takes two inputs arguments
     
     I. --config:  It takes a configuration file as authentication parameters and parameters are "Access Key" and "Secret Key".

     II. --state: It is an optional parameter, currently this package supports only minute level grouping refer (http://docs.retailnext.net/cloud/api) for more information about grouping options, however some metrics do not support minute level grouping hence it has been grouped to day level. State configuration file has three options:
		1) type: to specify day or miniute level grouping.
		2) increment: to specify number for increment. If type is day this parameter will work in day or if type is minute then it will work on minute level
                3) filter: This grouping options will have same parameter format as RetailNext accepts (refer http://docs.retailnext.net/cloud/api).

Note: For incremental options please update "increment" parameter in state file, this will be based on "type" parameter.

    
3. Running the application:
    > tap-retailnext  --config config.json  [--state  state.json]

