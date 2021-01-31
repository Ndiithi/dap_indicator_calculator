# MOH DAP Indicator calculation module.

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#license">License</a></li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project

The MoH DAP cornerstone source data sytem is the KHIS, an instance of the DHIS2 system. Indicators need to be integrated for further analyses.
Due to the extensive data required from KHIS, indicators for all orguntis across all periods, API calls may slow down the system henceforth the need to calculate
the indicators from a safe sanbox through direct database connection of KHIS.


<!-- GETTING STARTED -->
## Getting Started

This component uses antlr4 (https://www.antlr.org/) for evaluation of indicator expressions. It reuses precompiled lexer and parser from the
DHIS2 dhis2-antlr-expression-parser (https://github.com/dhis2/dhis2-antlr-expression-parser) component. 

### Prerequisites

Clone and install dhis2-antlr-expression-parser.

* git
  ```sh
  git clone https://github.com/dhis2/dhis2-antlr-expression-parser
  ```

* cd
  ```sh
  cd dhis2-antlr-expression-parser/
  ```

* maven
  ```sh
  mvn install
  ```

### Installation

1. Clone the repo
   ```sh
   https://github.com/uonafya/dap_indicator_calculator.git
   ```
2. Change directory.
   ```sh
   cd dap_indicator_calculator
   mvn package
   ```
3. Compile the package
   ```sh
   mvn package
   ```
Compile jar file is placed in the target directory.   

## Usage

To print help info run:

* java
  ```sh
  java -jar indicator_calculator.jar -h
  ```

The dap indicator calculator runs in two different modes:

Mode 1. Running specific user specified indicators for a given period and/or orgunit/orglevel only through a csv file.
Mode 2. Running all indicators from the KHIS database. This is the default mode for the app.

#### Command line parameters.
* -c : Continue processing from last point of processing. This generates a zipped file (processed_indicators.gzip) which stores status of already processed indicators. Take yes or no as argument.
* -from: Process indicators from this date. Date format is yyyy-mm-dd
* -to: Process indicators up to this date. Date format is yyyy-mm-dd
* -out: File name to store calculated indicators, eg /home/username/calculated.csv
* -level: The organizaton unit level to process the indicators, eg 2,1 or 5
* -in: Used with mode 1 where user specifies values to be processed. The input file should be in csv. The CSV file can be composed either one (only) of these groups of csv columns:
 
 1. (indicator,organisation, period) 
 2. (indicator,organisation_level, period) 
 3. (indicator). 
 
 The csv passed should have the names of the columns as listed above on the first row of the CSV file.
 
 - indicator: This column contains names of the indicators to process. The names should be as they appear on KHIS or the won't be processed.
 - organisation: Contains organization unit names to process the indicator, eg Kisii, Migori.
 - period: date of the format (yyyy-mm-dd) to process the indicator.
 - organisation_level: The immediate organisation units under one in this column will have its indicator in this row processed. Eg, if Kenya, then all counties will have the indicator specified in a particular row processed.
 
 If the csv file contains only the third option from the header groups above, ie, (indicator), All the indicators in this column for all orunits will be processed. This can however be controlled by the command line parameters.
 
 The periods to process indicators can be controlled by use of -to and -from cli parameters as defined above. The organization unit level to process can also be controlled
 with the -level parameter.
 
 Group 1 and 2 csv types (ie, csv with the colums "indicator,organisation, period" or "indicator,organisation_level, period") can not have the periods or organisation units controlled
 through the CLI parameters to,from or level. It only processes indicators, periods and orgunit given in the columns.

eg CSV file with indicaotor column only, process the indicators for orglevel 1 from the start date 2020-10-01 and should not save progress nor resume last progress(option -c no)
 ```sh
   java -jar target/indicator_calculator.jar -in /home/usename/Sheet.csv -c no -from 2020-10-01 -level 1
   ```

#### Defaults.
By default for the cammand line arguments are:
* -c true. strores progress in the root app folder with the name processed_indicators.gzip. If processed_indicators.gzip is deleted, the processing starts from begging.
* -from: all dates.
* -to: current date.
* -out: the root folder of the app jar file, with the name calculated_indicators.csv
* -level: all levels.
* -in: if none is given, processes all the KHIS indicators.


<!-- LICENSE -->
## License

Distributed under the GPL-3.0 License. See `LICENSE` for more information.
