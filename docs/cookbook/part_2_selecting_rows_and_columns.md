# CB-02: Selecting Rows and Columns

As in the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%202%20-%20Selecting%20data%20&%20finding%20the%20most%20common%20complaint%20type.ipynb), we are going to use the 311 service requests data from [NYC Open Data](https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9). We download the dataset using the `download-data!` utility function defined in Part 1:

```clojure
(def complaints-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/311-service-requests.csv")

(def complaints-data-path
  "data/cookbook/complaints.csv")

(download-data! complaints-data-url complaints-data-path)
=> :downloaded
```

and load the CSV dataset:

```clojure
(def raw-complaints (g/read-csv! complaints-data-path))
```

## 2.1 What's Even In It?

```clojure
(g/show raw-complaints)
; +----------+----------------------+----------------------+------+---------------------------------------+------------------------+------------------------------+-----------------------------+------------+-------------------------+------------------+----------------+--------------------------------+---------------------+---------------------+------------+-------------------+--------+-------------+--------+----------------------+------------------------------+--------------------+-------------+--------------------------+--------------------------+------------------+-------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
; |Unique Key|Created Date          |Closed Date           |Agency|Agency Name                            |Complaint Type          |Descriptor                    |Location Type                |Incident Zip|Incident Address         |Street Name       |Cross Street 1  |Cross Street 2                  |Intersection Street 1|Intersection Street 2|Address Type|City               |Landmark|Facility Type|Status  |Due Date              |Resolution Action Updated Date|Community Board     |Borough      |X Coordinate (State Plane)|Y Coordinate (State Plane)|Park Facility Name|Park Borough |School Name|School Number|School Region|School Code|School Phone Number|School Address|School City|School State|School Zip |School Not Found|School or Citywide Complaint|Vehicle Type|Taxi Company Borough|Taxi Pick Up Location|Bridge Highway Name|Bridge Highway Direction|Road Ramp|Bridge Highway Segment|Garage Lot Name|Ferry Direction|Ferry Terminal Name|Latitude          |Longitude         |Location                                |
; +----------+----------------------+----------------------+------+---------------------------------------+------------------------+------------------------------+-----------------------------+------------+-------------------------+------------------+----------------+--------------------------------+---------------------+---------------------+------------+-------------------+--------+-------------+--------+----------------------+------------------------------+--------------------+-------------+--------------------------+--------------------------+------------------+-------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
; |26589651  |10/31/2013 02:08:41 AM|null                  |NYPD  |New York City Police Department        |Noise - Street/Sidewalk |Loud Talking                  |Street/Sidewalk              |11432       |90-03 169 STREET         |169 STREET        |90 AVENUE       |91 AVENUE                       |null                 |null                 |ADDRESS     |JAMAICA            |null    |Precinct     |Assigned|10/31/2013 10:08:41 AM|10/31/2013 02:35:17 AM        |12 QUEENS           |QUEENS       |1042027                   |197389                    |Unspecified       |QUEENS       |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.70827532593202 |-73.79160395779721|(40.70827532593202, -73.79160395779721) |
; |26593698  |10/31/2013 02:01:04 AM|null                  |NYPD  |New York City Police Department        |Illegal Parking         |Commercial Overnight Parking  |Street/Sidewalk              |11378       |58 AVENUE                |58 AVENUE         |58 PLACE        |59 STREET                       |null                 |null                 |BLOCKFACE   |MASPETH            |null    |Precinct     |Open    |10/31/2013 10:01:04 AM|null                          |05 QUEENS           |QUEENS       |1009349                   |201984                    |Unspecified       |QUEENS       |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.721040535628305|-73.90945306791765|(40.721040535628305, -73.90945306791765)|
; |26594139  |10/31/2013 02:00:24 AM|10/31/2013 02:40:32 AM|NYPD  |New York City Police Department        |Noise - Commercial      |Loud Music/Party              |Club/Bar/Restaurant          |10032       |4060 BROADWAY            |BROADWAY          |WEST 171 STREET |WEST 172 STREET                 |null                 |null                 |ADDRESS     |NEW YORK           |null    |Precinct     |Closed  |10/31/2013 10:00:24 AM|10/31/2013 02:39:42 AM        |12 MANHATTAN        |MANHATTAN    |1001088                   |246531                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.84332975466513 |-73.93914371913482|(40.84332975466513, -73.93914371913482) |
; |26595721  |10/31/2013 01:56:23 AM|10/31/2013 02:21:48 AM|NYPD  |New York City Police Department        |Noise - Vehicle         |Car/Truck Horn                |Street/Sidewalk              |10023       |WEST 72 STREET           |WEST 72 STREET    |COLUMBUS AVENUE |AMSTERDAM AVENUE                |null                 |null                 |BLOCKFACE   |NEW YORK           |null    |Precinct     |Closed  |10/31/2013 09:56:23 AM|10/31/2013 02:21:10 AM        |07 MANHATTAN        |MANHATTAN    |989730                    |222727                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.7780087446372  |-73.98021349023975|(40.7780087446372, -73.98021349023975)  |
; |26590930  |10/31/2013 01:53:44 AM|null                  |DOHMH |Department of Health and Mental Hygiene|Rodent                  |Condition Attracting Rodents  |Vacant Lot                   |10027       |WEST 124 STREET          |WEST 124 STREET   |LENOX AVENUE    |ADAM CLAYTON POWELL JR BOULEVARD|null                 |null                 |BLOCKFACE   |NEW YORK           |null    |N/A          |Pending |11/30/2013 01:53:44 AM|10/31/2013 01:59:54 AM        |10 MANHATTAN        |MANHATTAN    |998815                    |233545                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.80769092704951 |-73.94738703491433|(40.80769092704951, -73.94738703491433) |
; |26592370  |10/31/2013 01:46:52 AM|null                  |NYPD  |New York City Police Department        |Noise - Commercial      |Banging/Pounding              |Club/Bar/Restaurant          |11372       |37 AVENUE                |37 AVENUE         |84 STREET       |85 STREET                       |null                 |null                 |BLOCKFACE   |JACKSON HEIGHTS    |null    |Precinct     |Open    |10/31/2013 09:46:52 AM|null                          |03 QUEENS           |QUEENS       |1016948                   |212540                    |Unspecified       |QUEENS       |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.7499893014072  |-73.88198770727831|(40.7499893014072, -73.88198770727831)  |
; |26595682  |10/31/2013 01:46:40 AM|null                  |NYPD  |New York City Police Department        |Blocked Driveway        |No Access                     |Street/Sidewalk              |11419       |107-50 109 STREET        |109 STREET        |107 AVENUE      |109 AVENUE                      |null                 |null                 |ADDRESS     |SOUTH RICHMOND HILL|null    |Precinct     |Assigned|10/31/2013 09:46:40 AM|10/31/2013 01:59:51 AM        |10 QUEENS           |QUEENS       |1030919                   |187622                    |Unspecified       |QUEENS       |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.68153278675525 |-73.83173699701601|(40.68153278675525, -73.83173699701601) |
; |26595195  |10/31/2013 01:44:19 AM|10/31/2013 01:58:49 AM|NYPD  |New York City Police Department        |Noise - Commercial      |Loud Music/Party              |Club/Bar/Restaurant          |11417       |137-09 CROSSBAY BOULEVARD|CROSSBAY BOULEVARD|PITKIN AVENUE   |VAN WICKLEN ROAD                |null                 |null                 |ADDRESS     |OZONE PARK         |null    |Precinct     |Closed  |10/31/2013 09:44:19 AM|10/31/2013 01:58:49 AM        |10 QUEENS           |QUEENS       |1027776                   |184076                    |Unspecified       |QUEENS       |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.67181584567338 |-73.84309181950769|(40.67181584567338, -73.84309181950769) |
; |26590540  |10/31/2013 01:44:14 AM|10/31/2013 02:28:04 AM|NYPD  |New York City Police Department        |Noise - Commercial      |Loud Talking                  |Club/Bar/Restaurant          |10011       |258 WEST 15 STREET       |WEST 15 STREET    |7 AVENUE        |8 AVENUE                        |null                 |null                 |ADDRESS     |NEW YORK           |null    |Precinct     |Closed  |10/31/2013 09:44:14 AM|10/31/2013 02:00:56 AM        |04 MANHATTAN        |MANHATTAN    |984031                    |208847                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.73991339303542 |-74.00079028612932|(40.73991339303542, -74.00079028612932) |
; |26594392  |10/31/2013 01:34:41 AM|10/31/2013 02:23:51 AM|NYPD  |New York City Police Department        |Noise - Commercial      |Loud Music/Party              |Club/Bar/Restaurant          |11225       |835 NOSTRAND AVENUE      |NOSTRAND AVENUE   |UNION STREET    |PRESIDENT STREET                |null                 |null                 |ADDRESS     |BROOKLYN           |null    |Precinct     |Closed  |10/31/2013 09:34:41 AM|10/31/2013 01:48:26 AM        |09 BROOKLYN         |BROOKLYN     |997941                    |182725                    |Unspecified       |BROOKLYN     |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.66820406598287 |-73.95064760056546|(40.66820406598287, -73.95064760056546) |
; |26595176  |10/31/2013 01:25:12 AM|null                  |NYPD  |New York City Police Department        |Noise - House of Worship|Loud Music/Party              |House of Worship             |11218       |3775 18 AVENUE           |18 AVENUE         |EAST 9 STREET   |EAST 8 STREET                   |null                 |null                 |ADDRESS     |BROOKLYN           |null    |Precinct     |Open    |10/31/2013 09:25:12 AM|null                          |14 BROOKLYN         |BROOKLYN     |992726                    |170399                    |Unspecified       |BROOKLYN     |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.63437840816299 |-73.96946177104543|(40.63437840816299, -73.96946177104543) |
; |26591982  |10/31/2013 01:24:14 AM|10/31/2013 01:54:39 AM|NYPD  |New York City Police Department        |Noise - Commercial      |Loud Music/Party              |Club/Bar/Restaurant          |10003       |187 2 AVENUE             |2 AVENUE          |EAST 11 STREET  |EAST 12 STREET                  |null                 |null                 |ADDRESS     |NEW YORK           |null    |Precinct     |Closed  |10/31/2013 09:24:14 AM|10/31/2013 01:54:39 AM        |03 MANHATTAN        |MANHATTAN    |988110                    |205533                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.73081644089586 |-73.98607265739876|(40.73081644089586, -73.98607265739876) |
; |26594169  |10/31/2013 01:20:57 AM|10/31/2013 02:12:31 AM|NYPD  |New York City Police Department        |Illegal Parking         |Double Parked Blocking Vehicle|Street/Sidewalk              |10029       |65 EAST 99 STREET        |EAST 99 STREET    |MADISON AVENUE  |PARK AVENUE                     |null                 |null                 |ADDRESS     |NEW YORK           |null    |Precinct     |Closed  |10/31/2013 09:20:57 AM|10/31/2013 01:42:05 AM        |11 MANHATTAN        |MANHATTAN    |997470                    |226725                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.78897400211689 |-73.95225898702977|(40.78897400211689, -73.95225898702977) |
; |26594391  |10/31/2013 01:20:13 AM|null                  |NYPD  |New York City Police Department        |Noise - Vehicle         |Engine Idling                 |Street/Sidewalk              |10466       |null                     |null              |null            |null                            |STRANG AVENUE        |AMUNDSON AVENUE      |INTERSECTION|BRONX              |null    |Precinct     |Open    |10/31/2013 09:20:13 AM|null                          |12 BRONX            |BRONX        |1029467                   |264124                    |Unspecified       |BRONX        |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.89151738488846 |-73.83645714593568|(40.89151738488846, -73.83645714593568) |
; |26590917  |10/31/2013 01:19:54 AM|null                  |DOHMH |Department of Health and Mental Hygiene|Rodent                  |Rat Sighting                  |1-2 Family Mixed Use Building|11219       |63 STREET                |63 STREET         |13 AVENUE       |14 AVENUE                       |null                 |null                 |BLOCKFACE   |BROOKLYN           |null    |N/A          |Pending |11/30/2013 01:19:54 AM|10/31/2013 01:29:26 AM        |10 BROOKLYN         |BROOKLYN     |984467                    |167519                    |Unspecified       |BROOKLYN     |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.6264774690411  |-73.99921826202639|(40.6264774690411, -73.99921826202639)  |
; |26591458  |10/31/2013 01:14:02 AM|10/31/2013 01:30:34 AM|NYPD  |New York City Police Department        |Noise - House of Worship|Loud Music/Party              |House of Worship             |10025       |null                     |null              |null            |null                            |WEST   99 STREET     |BROADWAY             |INTERSECTION|NEW YORK           |null    |Precinct     |Closed  |10/31/2013 09:14:02 AM|10/31/2013 01:30:34 AM        |07 MANHATTAN        |MANHATTAN    |992454                    |229500                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.7965967075252  |-73.97036973473399|(40.7965967075252, -73.97036973473399)  |
; |26594086  |10/31/2013 12:54:03 AM|10/31/2013 02:16:39 AM|NYPD  |New York City Police Department        |Noise - Street/Sidewalk |Loud Music/Party              |Street/Sidewalk              |10310       |173 CAMPBELL AVENUE      |CAMPBELL AVENUE   |HENDERSON AVENUE|WINEGAR LANE                    |null                 |null                 |ADDRESS     |STATEN ISLAND      |null    |Precinct     |Closed  |10/31/2013 08:54:03 AM|10/31/2013 02:07:14 AM        |01 STATEN ISLAND    |STATEN ISLAND|952013                    |171076                    |Unspecified       |STATEN ISLAND|Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.63618202176914 |-74.1161500428337 |(40.63618202176914, -74.1161500428337)  |
; |26595117  |10/31/2013 12:52:46 AM|null                  |NYPD  |New York City Police Department        |Illegal Parking         |Posted Parking Sign Violation |Street/Sidewalk              |11236       |null                     |null              |null            |null                            |ROCKAWAY PARKWAY     |SKIDMORE AVENUE      |INTERSECTION|BROOKLYN           |null    |Precinct     |Open    |10/31/2013 08:52:46 AM|null                          |18 BROOKLYN         |BROOKLYN     |1015289                   |169710                    |Unspecified       |BROOKLYN     |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.63243692394328 |-73.88817263437012|(40.63243692394328, -73.88817263437012) |
; |26590389  |10/31/2013 12:51:00 AM|null                  |DOT   |Department of Transportation           |Street Light Condition  |Street Light Out              |null                         |null        |226 42 ST E              |42 ST E           |CHURCH AVE      |SNYDER AVE                      |null                 |null                 |ADDRESS     |null               |null    |N/A          |Open    |null                  |null                          |Unspecified BROOKLYN|BROOKLYN     |null                      |null                      |Unspecified       |BROOKLYN     |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|null            |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |null              |null              |null                                    |
; |26594210  |10/31/2013 12:46:27 AM|null                  |NYPD  |New York City Police Department        |Noise - Commercial      |Loud Music/Party              |Club/Bar/Restaurant          |10033       |null                     |null              |null            |null                            |WEST  184 STREET     |BROADWAY             |INTERSECTION|NEW YORK           |null    |Precinct     |Assigned|10/31/2013 08:46:27 AM|10/31/2013 01:32:41 AM        |12 MANHATTAN        |MANHATTAN    |1002294                   |249712                    |Unspecified       |MANHATTAN    |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.85205827756883 |-73.93477640780834|(40.85205827756883, -73.93477640780834) |
; +----------+----------------------+----------------------+------+---------------------------------------+------------------------+------------------------------+-----------------------------+------------+-------------------------+------------------+----------------+--------------------------------+---------------------+---------------------+------------+-------------------+--------+-------------+--------+----------------------+------------------------------+--------------------+-------------+--------------------------+--------------------------+------------------+-------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
; only showing top 20 rows
```

By default, Spark only shows the top 20 rows. We can see that there are 111069 rows and 52 columns:

```clojure
(g/count raw-complaints)
=> 111069

(count (g/columns raw-complaints))
=> 52
```

As before, we can check out the schema:

```clojure
; root
;  |-- Unique Key: string (nullable = true)
;  |-- Created Date: string (nullable = true)
;  |-- Closed Date: string (nullable = true)
;  |-- Agency: string (nullable = true)
;  |-- Agency Name: string (nullable = true)
;  |-- Complaint Type: string (nullable = true)
;  |-- Descriptor: string (nullable = true)
;  |-- Location Type: string (nullable = true)
;  |-- Incident Zip: string (nullable = true)
;  |-- Incident Address: string (nullable = true)
;  |-- Street Name: string (nullable = true)
;  |-- Cross Street 1: string (nullable = true)
;  |-- Cross Street 2: string (nullable = true)
;  |-- Intersection Street 1: string (nullable = true)
;  |-- Intersection Street 2: string (nullable = true)
;  |-- Address Type: string (nullable = true)
;  |-- City: string (nullable = true)
;  |-- Landmark: string (nullable = true)
;  |-- Facility Type: string (nullable = true)
;  |-- Status: string (nullable = true)
;  |-- Due Date: string (nullable = true)
;  |-- Resolution Action Updated Date: string (nullable = true)
;  |-- Community Board: string (nullable = true)
;  |-- Borough: string (nullable = true)
;  |-- X Coordinate (State Plane): string (nullable = true)
;  |-- Y Coordinate (State Plane): string (nullable = true)
;  |-- Park Facility Name: string (nullable = true)
;  |-- Park Borough: string (nullable = true)
;  |-- School Name: string (nullable = true)
;  |-- School Number: string (nullable = true)
;  |-- School Region: string (nullable = true)
;  |-- School Code: string (nullable = true)
;  |-- School Phone Number: string (nullable = true)
;  |-- School Address: string (nullable = true)
;  |-- School City: string (nullable = true)
;  |-- School State: string (nullable = true)
;  |-- School Zip: string (nullable = true)
;  |-- School Not Found: string (nullable = true)
;  |-- School or Citywide Complaint: string (nullable = true)
;  |-- Vehicle Type: string (nullable = true)
;  |-- Taxi Company Borough: string (nullable = true)
;  |-- Taxi Pick Up Location: string (nullable = true)
;  |-- Bridge Highway Name: string (nullable = true)
;  |-- Bridge Highway Direction: string (nullable = true)
;  |-- Road Ramp: string (nullable = true)
;  |-- Bridge Highway Segment: string (nullable = true)
;  |-- Garage Lot Name: string (nullable = true)
;  |-- Ferry Direction: string (nullable = true)
;  |-- Ferry Terminal Name: string (nullable = true)
;  |-- Latitude: string (nullable = true)
;  |-- Longitude: string (nullable = true)
;  |-- Location: string (nullable = true)
```

The fact that the column names are not in kebab case is typical.

Most datasets we see will not have kebab-case columns. We can deal with it programmatically by i) converting to kebab case and ii) deleting everything inside parantheses. For the former, we can use the awesome [camel-snake-kebab](https://github.com/clj-commons/camel-snake-kebab) library, and for the latter, we do a simple regex replace. We define a new utility function `normalise-column-names`:

```clojure
(require '[camel-snake-kebab.core])
(require '[clojure.string])

(defn normalise-column-names [dataset]
  (let [new-columns (->> dataset
                         g/column-names
                         (map #(clojure.string/replace % #"\((.*?)\)" ""))
                         (map #(clojure.string/replace % #"/" ""))
                         (map camel-snake-kebab.core/->kebab-case))]
    (g/to-df dataset new-columns)))

(def complaints (normalise-column-names raw-complaints))

(g/print-schema complaints)
; root
;  |-- unique-key: string (nullable = true)
;  |-- created-date: string (nullable = true)
;  |-- closed-date: string (nullable = true)
;  |-- agency: string (nullable = true)
;  |-- agency-name: string (nullable = true)
;  |-- complaint-type: string (nullable = true)
;  |-- descriptor: string (nullable = true)
;  |-- location-type: string (nullable = true)
;  |-- incident-zip: string (nullable = true)
;  |-- incident-address: string (nullable = true)
;  |-- street-name: string (nullable = true)
;  |-- cross-street-1: string (nullable = true)
;  |-- cross-street-2: string (nullable = true)
;  |-- intersection-street-1: string (nullable = true)
;  |-- intersection-street-2: string (nullable = true)
;  |-- address-type: string (nullable = true)
;  |-- city: string (nullable = true)
;  |-- landmark: string (nullable = true)
;  |-- facility-type: string (nullable = true)
;  |-- status: string (nullable = true)
;  |-- due-date: string (nullable = true)
;  |-- resolution-action-updated-date: string (nullable = true)
;  |-- community-board: string (nullable = true)
;  |-- borough: string (nullable = true)
;  |-- x-coordinate: string (nullable = true)
;  |-- y-coordinate: string (nullable = true)
;  |-- park-facility-name: string (nullable = true)
;  |-- park-borough: string (nullable = true)
;  |-- school-name: string (nullable = true)
;  |-- school-number: string (nullable = true)
;  |-- school-region: string (nullable = true)
;  |-- school-code: string (nullable = true)
;  |-- school-phone-number: string (nullable = true)
;  |-- school-address: string (nullable = true)
;  |-- school-city: string (nullable = true)
;  |-- school-state: string (nullable = true)
;  |-- school-zip: string (nullable = true)
;  |-- school-not-found: string (nullable = true)
;  |-- school-or-citywide-complaint: string (nullable = true)
;  |-- vehicle-type: string (nullable = true)
;  |-- taxi-company-borough: string (nullable = true)
;  |-- taxi-pick-up-location: string (nullable = true)
;  |-- bridge-highway-name: string (nullable = true)
;  |-- bridge-highway-direction: string (nullable = true)
;  |-- road-ramp: string (nullable = true)
;  |-- bridge-highway-segment: string (nullable = true)
;  |-- garage-lot-name: string (nullable = true)
;  |-- ferry-direction: string (nullable = true)
;  |-- ferry-terminal-name: string (nullable = true)
;  |-- latitude: string (nullable = true)
;  |-- longitude: string (nullable = true)
;  |-- location: string (nullable = true)
```

Note that the function `g/to-df` may take, as arguments, column names in a variadic way, a list of column names or a combination of the two.

We additionally remove the `/` character, as it may cause problems with Clojure namespaces.

## 2.2 Selecting Columns and Rows

To select a column, we use `g/select` as in the previous part:

```clojure
(-> complaints
    (g/select :complaint-type)
    g/show)
; +------------------------+
; |complaint-type          |
; +------------------------+
; |Noise - Street/Sidewalk |
; |Illegal Parking         |
; |Noise - Commercial      |
; |Noise - Vehicle         |
; |Rodent                  |
; |Noise - Commercial      |
; |Blocked Driveway        |
; |Noise - Commercial      |
; |Noise - Commercial      |
; |Noise - Commercial      |
; |Noise - House of Worship|
; |Noise - Commercial      |
; |Illegal Parking         |
; |Noise - Vehicle         |
; |Rodent                  |
; |Noise - House of Worship|
; |Noise - Street/Sidewalk |
; |Illegal Parking         |
; |Street Light Condition  |
; |Noise - Commercial      |
; +------------------------+
; only showing top 20 rows
```

To only see the top five rows, we use `g/limit`:

```clojure
; +----------+----------------------+----------------------+------+---------------------------------------+-----------------------+----------------------------+-------------------+------------+----------------+---------------+---------------+--------------------------------+---------------------+---------------------+------------+--------+--------+-------------+--------+----------------------+------------------------------+---------------+---------+------------+------------+------------------+------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
; |unique-key|created-date          |closed-date           |agency|agency-name                            |complaint-type         |descriptor                  |location-type      |incident-zip|incident-address|street-name    |cross-street-1 |cross-street-2                  |intersection-street-1|intersection-street-2|address-type|city    |landmark|facility-type|status  |due-date              |resolution-action-updated-date|community-board|borough  |x-coordinate|y-coordinate|park-facility-name|park-borough|school-name|school-number|school-region|school-code|school-phone-number|school-address|school-city|school-state|school-zip |school-not-found|school-or-citywide-complaint|vehicle-type|taxi-company-borough|taxi-pick-up-location|bridge-highway-name|bridge-highway-direction|road-ramp|bridge-highway-segment|garage-lot-name|ferry-direction|ferry-terminal-name|latitude          |longitude         |location                                |
; +----------+----------------------+----------------------+------+---------------------------------------+-----------------------+----------------------------+-------------------+------------+----------------+---------------+---------------+--------------------------------+---------------------+---------------------+------------+--------+--------+-------------+--------+----------------------+------------------------------+---------------+---------+------------+------------+------------------+------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
; |26589651  |10/31/2013 02:08:41 AM|null                  |NYPD  |New York City Police Department        |Noise - Street/Sidewalk|Loud Talking                |Street/Sidewalk    |11432       |90-03 169 STREET|169 STREET     |90 AVENUE      |91 AVENUE                       |null                 |null                 |ADDRESS     |JAMAICA |null    |Precinct     |Assigned|10/31/2013 10:08:41 AM|10/31/2013 02:35:17 AM        |12 QUEENS      |QUEENS   |1042027     |197389      |Unspecified       |QUEENS      |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.70827532593202 |-73.79160395779721|(40.70827532593202, -73.79160395779721) |
; |26593698  |10/31/2013 02:01:04 AM|null                  |NYPD  |New York City Police Department        |Illegal Parking        |Commercial Overnight Parking|Street/Sidewalk    |11378       |58 AVENUE       |58 AVENUE      |58 PLACE       |59 STREET                       |null                 |null                 |BLOCKFACE   |MASPETH |null    |Precinct     |Open    |10/31/2013 10:01:04 AM|null                          |05 QUEENS      |QUEENS   |1009349     |201984      |Unspecified       |QUEENS      |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.721040535628305|-73.90945306791765|(40.721040535628305, -73.90945306791765)|
; |26594139  |10/31/2013 02:00:24 AM|10/31/2013 02:40:32 AM|NYPD  |New York City Police Department        |Noise - Commercial     |Loud Music/Party            |Club/Bar/Restaurant|10032       |4060 BROADWAY   |BROADWAY       |WEST 171 STREET|WEST 172 STREET                 |null                 |null                 |ADDRESS     |NEW YORK|null    |Precinct     |Closed  |10/31/2013 10:00:24 AM|10/31/2013 02:39:42 AM        |12 MANHATTAN   |MANHATTAN|1001088     |246531      |Unspecified       |MANHATTAN   |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.84332975466513 |-73.93914371913482|(40.84332975466513, -73.93914371913482) |
; |26595721  |10/31/2013 01:56:23 AM|10/31/2013 02:21:48 AM|NYPD  |New York City Police Department        |Noise - Vehicle        |Car/Truck Horn              |Street/Sidewalk    |10023       |WEST 72 STREET  |WEST 72 STREET |COLUMBUS AVENUE|AMSTERDAM AVENUE                |null                 |null                 |BLOCKFACE   |NEW YORK|null    |Precinct     |Closed  |10/31/2013 09:56:23 AM|10/31/2013 02:21:10 AM        |07 MANHATTAN   |MANHATTAN|989730      |222727      |Unspecified       |MANHATTAN   |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.7780087446372  |-73.98021349023975|(40.7780087446372, -73.98021349023975)  |
; |26590930  |10/31/2013 01:53:44 AM|null                  |DOHMH |Department of Health and Mental Hygiene|Rodent                 |Condition Attracting Rodents|Vacant Lot         |10027       |WEST 124 STREET |WEST 124 STREET|LENOX AVENUE   |ADAM CLAYTON POWELL JR BOULEVARD|null                 |null                 |BLOCKFACE   |NEW YORK|null    |N/A          |Pending |11/30/2013 01:53:44 AM|10/31/2013 01:59:54 AM        |10 MANHATTAN   |MANHATTAN|998815      |233545      |Unspecified       |MANHATTAN   |Unspecified|Unspecified  |Unspecified  |Unspecified|Unspecified        |Unspecified   |Unspecified|Unspecified |Unspecified|N               |null                        |null        |null                |null                 |null               |null                    |null     |null                  |null           |null           |null               |40.80769092704951 |-73.94738703491433|(40.80769092704951, -73.94738703491433) |
; +----------+----------------------+----------------------+------+---------------------------------------+-----------------------+----------------------------+-------------------+------------+----------------+---------------+---------------+--------------------------------+---------------------+---------------------+------------+--------+--------+-------------+--------+----------------------+------------------------------+---------------+---------+------------+------------+------------------+------------+-----------+-------------+-------------+-----------+-------------------+--------------+-----------+------------+-----------+----------------+----------------------------+------------+--------------------+---------------------+-------------------+------------------------+---------+----------------------+---------------+---------------+-------------------+------------------+------------------+----------------------------------------+
```

We can chain the two commands together:

```clojure
(-> complaints
    (g/select :complaint-type)
    (g/limit 5)
    g/show)
; +-----------------------+
; |complaint-type         |
; +-----------------------+
; |Noise - Street/Sidewalk|
; |Illegal Parking        |
; |Noise - Commercial     |
; |Noise - Vehicle        |
; |Rodent                 |
; +-----------------------+
```

and the order does not matter:

```clojure
(-> complaints
    (g/limit 5)
    (g/select :complaint-type)
    g/show)
; +-----------------------+
; |complaint-type         |
; +-----------------------+
; |Noise - Street/Sidewalk|
; |Illegal Parking        |
; |Noise - Commercial     |
; |Noise - Vehicle        |
; |Rodent                 |
; +-----------------------+
```
## 2.3 Selecting Multiple Columns

The function `g/select` is variadic:

```clojure
(-> complaints
    (g/select :complaint-type :borough)
    g/show)
; +------------------------+-------------+
; |complaint-type          |borough      |
; +------------------------+-------------+
; |Noise - Street/Sidewalk |QUEENS       |
; |Illegal Parking         |QUEENS       |
; |Noise - Commercial      |MANHATTAN    |
; |Noise - Vehicle         |MANHATTAN    |
; |Rodent                  |MANHATTAN    |
; |Noise - Commercial      |QUEENS       |
; |Blocked Driveway        |QUEENS       |
; |Noise - Commercial      |QUEENS       |
; |Noise - Commercial      |MANHATTAN    |
; |Noise - Commercial      |BROOKLYN     |
; |Noise - House of Worship|BROOKLYN     |
; |Noise - Commercial      |MANHATTAN    |
; |Illegal Parking         |MANHATTAN    |
; |Noise - Vehicle         |BRONX        |
; |Rodent                  |BROOKLYN     |
; |Noise - House of Worship|MANHATTAN    |
; |Noise - Street/Sidewalk |STATEN ISLAND|
; |Illegal Parking         |BROOKLYN     |
; |Street Light Condition  |BROOKLYN     |
; |Noise - Commercial      |MANHATTAN    |
; +------------------------+-------------+
; only showing top 20 rows
```

As before, we can arbitrarily compose `g/limit`:

```clojure
(-> complaints
    (g/select :complaint-type :borough)
    (g/limit 10)
    g/show)
; +-----------------------+---------+
; |complaint-type         |borough  |
; +-----------------------+---------+
; |Noise - Street/Sidewalk|QUEENS   |
; |Illegal Parking        |QUEENS   |
; |Noise - Commercial     |MANHATTAN|
; |Noise - Vehicle        |MANHATTAN|
; |Rodent                 |MANHATTAN|
; |Noise - Commercial     |QUEENS   |
; |Blocked Driveway       |QUEENS   |
; |Noise - Commercial     |QUEENS   |
; |Noise - Commercial     |MANHATTAN|
; |Noise - Commercial     |BROOKLYN |
; +-----------------------+---------+
```

## 2.4 What's The Most Common Complaint Types?

To count the number of rows per unique value of a column, we can compose `g/group-by` and `g/count`:

```clojure
(-> complaints
    (g/group-by :complaint-type)
    g/count
    g/show)
; +------------------------+-----+
; |complaint-type          |count|
; +------------------------+-----+
; |Traffic Signal Condition|3145 |
; |Cranes and Derricks     |10   |
; |ELECTRIC                |2350 |
; |Noise - Helicopter      |99   |
; |STRUCTURAL              |16   |
; |Fire Alarm - New System |6    |
; |Window Guard            |2    |
; |Broken Muni Meter       |2070 |
; |Highway Condition       |130  |
; |Street Condition        |3473 |
; |DOF Literature Request  |5797 |
; |Hazardous Materials     |171  |
; |Vending                 |229  |
; |Ferry Permit            |1    |
; |PAINT - PLASTER         |5149 |
; |DFTA Literature Request |2    |
; |Drinking Water          |11   |
; |Public Toilet           |6    |
; |DPR Literature Request  |7    |
; |GENERAL CONSTRUCTION    |7471 |
; +------------------------+-----+
; only showing top 20 rows
```

This works, but we are probably interested only in the top 10 complaint types sorted in descending order. To achieve this, we can always compose `g/limit` and `g/order-by` arbitrarily:

```clojure
(-> complaints
    (g/group-by :complaint-type)
    g/count
    (g/order-by (g/desc :count))
    (g/limit 10)
    g/show)
; +----------------------+-----+
; |complaint-type        |count|
; +----------------------+-----+
; |HEATING               |14200|
; |GENERAL CONSTRUCTION  |7471 |
; |Street Light Condition|7117 |
; |DOF Literature Request|5797 |
; |PLUMBING              |5373 |
; |PAINT - PLASTER       |5149 |
; |Blocked Driveway      |4590 |
; |NONCONST              |3998 |
; |Street Condition      |3473 |
; |Illegal Parking       |3343 |
; +----------------------+-----+
```

This operation is so common that we have a shortcut function, namely `g/value-counts`:

```clojure
(-> complaints
    (g/select :complaint-type)
    g/value-counts
    (g/limit 10)
    g/show)
; +----------------------+-----+
; |complaint-type        |count|
; +----------------------+-----+
; |HEATING               |14200|
; |GENERAL CONSTRUCTION  |7471 |
; |Street Light Condition|7117 |
; |DOF Literature Request|5797 |
; |PLUMBING              |5373 |
; |PAINT - PLASTER       |5149 |
; |Blocked Driveway      |4590 |
; |NONCONST              |3998 |
; |Street Condition      |3473 |
; |Illegal Parking       |3343 |
; +----------------------+-----+
```

## 2.5 Selecting Only Noise Complaints

To select for certain rows with a specific column value, we can use `g/filter` and a boolean expression. For instance, the following filters for rows that indicate street-noise complaints:

```clojure
(-> complaints
    (g/filter (g/= :complaint-type (g/lit "Noise - Street/Sidewalk")))
    (g/select :complaint-type :borough :created-date :descriptor)
    (g/limit 3)
    g/show)
; +-----------------------+-------------+----------------------+----------------+
; |complaint-type         |borough      |created-date          |descriptor      |
; +-----------------------+-------------+----------------------+----------------+
; |Noise - Street/Sidewalk|QUEENS       |10/31/2013 02:08:41 AM|Loud Talking    |
; |Noise - Street/Sidewalk|STATEN ISLAND|10/31/2013 12:54:03 AM|Loud Music/Party|
; |Noise - Street/Sidewalk|STATEN ISLAND|10/31/2013 12:35:18 AM|Loud Talking    |
; +-----------------------+-------------+----------------------+----------------+
```

In the form `(g/= :complaint-type (g/lit "Noise - Street/Sidewalk"))`, `:complaint-type` refers to the column named as such and `(g/lit "Noise - Street/Sidewalk")` refers to the literal value of the string `"Noise - Street/Sidewalk"`. Note that we need the `g/lit` there to indicate that it is a literal value, otherwise it will be interpreted as a column.

To combine two boolean expressions, we can use `g/&&` and `g/||` for "and" and "or" respectively. For example, to see street-noise complaints in Brooklyn:

```clojure
(-> complaints
    (g/filter (g/&&
                (g/= :complaint-type (g/lit "Noise - Street/Sidewalk"))
                (g/= :borough (g/lit "BROOKLYN"))))
    (g/select :complaint-type :borough :created-date :descriptor)
    (g/limit 3)
    g/show)
; +-----------------------+--------+----------------------+----------------+
; |complaint-type         |borough |created-date          |descriptor      |
; +-----------------------+--------+----------------------+----------------+
; |Noise - Street/Sidewalk|BROOKLYN|10/31/2013 12:30:36 AM|Loud Music/Party|
; |Noise - Street/Sidewalk|BROOKLYN|10/31/2013 12:05:10 AM|Loud Talking    |
; |Noise - Street/Sidewalk|BROOKLYN|10/30/2013 11:26:32 PM|Loud Music/Party|
; +-----------------------+--------+----------------------+----------------+
```

## 2.6 Which Borough Has The Most Noise Complaints?

To answer this question, we can simply compose the functions from the last two sub-sections:

```clojure
(-> complaints
    (g/filter (g/= :complaint-type (g/lit "Noise - Street/Sidewalk")))
    (g/select :borough)
    g/value-counts
    g/show)
; +-------------+-----+
; |borough      |count|
; +-------------+-----+
; |MANHATTAN    |917  |
; |BROOKLYN     |456  |
; |BRONX        |292  |
; |QUEENS       |226  |
; |STATEN ISLAND|36   |
; |Unspecified  |1    |
; +-------------+-----+
```
