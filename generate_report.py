#!/usr/bin/python
import os
import sys
import argparse
import logging
from datetime import datetime
from logger import setup_logging, LOGGER_NAME
from excel_writer import ExcelWriter
from vertica import VerticaDatabase
from report_helper import *
from utility import *

logger = logging.getLogger(LOGGER_NAME)


class NFArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

    def initialize(self):
        group = self.add_argument_group('Report generation inputs')
        group.add_argument('-s', '--server',
                           action="store",
                           help="Vertica DB Server Address/Name",
                           dest="server")

        group.add_argument('-u', '--user',
                           action="store",
                           help="User Name to access Vertica DB Server",
                           dest="username")

        group.add_argument('-p', '--password',
                           action="store",
                           help="Password to access Vertica DB Server",
                           dest="password")

        group.add_argument('-c', '--customer',
                           action="store",
                           help="Customer ID",
                           dest="customer")

        group.add_argument('-d', '--device_type_id',
                           action="store",
                           help="Comma separated  list of DeviceType ID",
                           default=30,
                           dest="device_type_id")

        group.add_argument('-t', '--start_date',
                           action="store",
                           help="Start Date(MM-DD-YYYY)",
                           default=datetime.now().date().strftime("%m-%d-%Y"),
                           dest="start_date")

        group.add_argument('-f', '--csv_file',
                           action="store",
                           help="CSV File",
                           dest="csv_file")

        self.add_argument('-v', '--verbose',
                          action="store_true",
                          default=False,
                          help="Print verbose logging on screen")

        if len(sys.argv) == 1:
            self.print_help()
            sys.exit(1)

    def validate(self):
        parsed_result = self.parse_args()
        if not parsed_result.server:
            self.error("Missing Vertica DB Server Address")
        if not parsed_result.username:
            self.error("Missing Vertica DB UserName")
        if not parsed_result.password:
            self.error("Missing Vertica DB Password")
        if not parsed_result.customer:
            self.error("Missing Customer ID")
        if not parsed_result.start_date:
            self.error("Missing Start Date(MM-DD-YYYY)")
        if not parsed_result.csv_file:
            self.error("Missing CSV file.")
        if not validate_date(parsed_result.start_date):
            self.error("Incorrect date format. It should be MM-DD-YYYY.")
        return parsed_result


def do_input_validation():
    parser = NFArgumentParser(add_help=True)
    parser.initialize()
    return parser.validate()


def main():
    root_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root_path)
    result = do_input_validation()
    result.verbose = True
    setup_logging(logfile="bt_report.log", scrnlog=result.verbose)
    logger.info("Running Script with -> Vertica Server: %s, Vertica User: %s, Customer ID: %s, DeviceType ID: %s, "
                "Date: %s, CSV File: %s, Verbose: %s" % (result.server, result.username, result.customer,
                                                         result.device_type_id, result.start_date, result.csv_file,
                                                         result.verbose))
    date_list, end_date = get_dates(start_date=result.start_date)
    date_formatter = datetime.strptime(end_date, TIME_FORMAT_MDY)
    year_month_format = "%s%02d" % (date_formatter.year, date_formatter.month)
    report_name = "50_Network Intrusion Prevention_Detection Service Report_%s" % year_month_format
    report_output_file_path = get_output_file_path(base_path=root_path, template_file=report_name)
    logger.info("Report Generation Started. Result file: %s" % report_output_file_path)
    # print report_output_file_path
    try:
        vertica_db_instance = VerticaDatabase(
            server=result.server,
            user=result.username,
            password=result.password
        )
        final_data_dict, severity_records, top_5_alarms = fetch_records(
            db_instance=vertica_db_instance,
            customer_id=result.customer,
            device_type_ids=result.device_type_id,
            start_date=result.start_date,
            end_date=end_date,
            date_range=date_list,
            csv_file_path=result.csv_file
        )
        # print top_5_alarms
        # print final_data_dict
        workbook = ExcelWriter(report_output_file_path)
        sheet_name = "50_NIDS_IPS_Report_%s" % year_month_format
        workbook.write_to_document_file(sheet_name=sheet_name, date_str=end_date)
        workbook.write_data_worksheet(sheet_name="DATA", data=final_data_dict, top_alarms=top_5_alarms)
        workbook.draw_top5_charts(sheet_name="TOP 5")
        workbook.write_main_worksheet(sheet_name="MAIN", data=severity_records, start_date=result.start_date,
                                      end_date=end_date)
        workbook.close()
        logger.info("Report Generation Completed. Result file: %s" % report_output_file_path)
        print("Report Generation Completed. Result file: %s" % report_output_file_path)
    except Exception, ex:
        logger.exception(ex)
        sys.exit()


if __name__ == '__main__':
    main()
