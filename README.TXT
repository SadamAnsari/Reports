BT_REPORTS Package 

Version 1.0.0

System Requirements
	Python 2.7 and above
	Linux operating system

Python Libraries
	vertica-python
	xlsxwriter
	dateutil
	pytz 
	future
	six

Installation
	- Extract the module to a location
	- Run generate_report.py with below arguments:
		* generate_report.py -s <Vertica DB Server> -u <Vertica DB Username> -p <Vertica DB Password> -c <Customer ID> -d 
								<Device Type IDs> -t <Date> -f <CSV_File>
		
		* Mandatory arguments:
			1. <Vertica DB Server>
			2. <Vertica DB Username> 
			3. <Vertica DB Password>
			4. <Customer ID>
			5. <CSV_File>
		
		* Optional Arguments:
			1. <Device Type IDs>  : It will take default value 30 if not given as argument.
			2. <Date> : It will take Current Date in MM-DD-YYYY format if not given as arguments.
		
	- Report will be generated inside "Reports" directory.
	   