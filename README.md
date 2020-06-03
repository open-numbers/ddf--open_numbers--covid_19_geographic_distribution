# ECDC geographic distribution COVID-19 cases

ECDC's aggregation of world wide reported covid-19 cases, updated daily.

## Data source

All data comes from [ECDC's coronavirus web site](https://www.ecdc.europa.eu/en/coronavirus), specifically the [csv formatted file](https://opendata.ecdc.europa.eu/covid19/casedistribution/csv) on the [Download today’s data on the geographic distribution of COVID-19 cases worldwide](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) page.

ECDC updates its data daily. We update this dataset set hourly. Therefore there may be a delay between the ECDC website and our dataset of one hour.

## Additional calculated indicators
 - `seven_day_rolling_average_deaths` 7 day window from given day back, averaged
 - `fourteen_day_rolling_average_deaths` 14 day window from given day back, averaged
 - `eight_to_fourteen_days_ago_average_deaths` 2 * 14 day avg - 7 day avg
 - `week_growth_rate` 7 day avg / 8 to 14 day avg

## Open Numbers
This is [ddf--ecdc--covid_19_geographic_distribution](https://github.com/open-numbers/ddf--ecdc--covid_19_geographic_distribution) harmonized to the Open Numbers name space. This means it uses the same identifiers as all other Open Numbers datasets and can thus easily be analyzed and visualized with other datasets.
