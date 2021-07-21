# AT Internet Analytics Export 

Script for fetching analytics data from AT Internet platform


## Tables
The requests are data driven.

ata.sites: List of "sites" from AT Analytics.  Each site within your account will have a code, which is stored as ata_code
ata.columns: List of columns from AT Analytics. Each metric/dimension that needs to be exported should be stored here using the column name from AT.
ata.requests: All requests that will be sent to AT Analytics.  Define which table the data should import to, and whether the data should be fetched one day at a time and/or in monthly/yearly periods
** this is a custom use case (data will be fetched for the period of each individual month, as well as the year as a whole) which can be modified for your personal situation
* if run_yearly = 1, the records from the entire year will be fetched for the cut of data labelled "All"
* if run_monthly = 1, the records from the current month will be fetched for the cut of data labelled as the currenet month, e.g "July"
* if run_daily = 1, the records will only be fetched from the previous day rather than the entire month/year (this should be used in conjunction with the data metric in your data request)
* if run_daily = 0, the records will be fetched for the full month/year based on settings

ata.request_sites: Each request needs at least 1 site associated with it and can accept multiple sites
ata.request_columns: Each request needs at least 1 column associated with it and can accept multiple columns (ordering doesn't matter)