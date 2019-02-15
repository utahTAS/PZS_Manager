# PZS_Manager
Check daily PZS for failures, create email report. Can check past PZS as well. When the admin mode is activated this program will execute and analysis of the last two weeks (and month) of PZSs. It generates an email that contains a list of warnings and failures for the last two weeks PZSs. It will highlight the ones that were run that day. The most important part of the email is where it shows month based PZS gaps, and any gaps in the last two weeks or seven days. 

## Getting Started

Just download the latest version here. The source coed will be included, but  a stand alone binary file will also be provided which will not require the user to have python or a python IDE installed.

### Prerequisites

No prereqs needed. Just download the .exe and run it. 

```
Give examples
```

### Installing

No installation needed. Just download the .exe and run it. 


## Running the tests

You can test the application by clicking run with the "Send Email" checkbox checked. This will send an email to me at bcubrich@utah.gov by default, so to perform the test you will want to change the "Preview Email:" entry field to your own email. 

### Break down into end to end tests

Explain what these tests test and why

This will test the server connection, email connection, and the integrity of the download of the script.



## Built With

Python - Entire Scirpt
Pandas
MIMEtext
tkinter
numpy        
pyodbc 
datetime  
BeautifulSoup



## Versioning

Versioning method undetermined at this time. Update soon. Currently just releasinf the first stable version as V1.

## Authors

* **Bart Cubrich** - *Initial work* 

## Acknowledgments

*Phil Harrison 
* Krisy Weber
