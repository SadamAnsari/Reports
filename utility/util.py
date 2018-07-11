#!/usr/bin/python
import re
import os
import csv
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from logger import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

TIME_FORMAT_MDY = "%m-%d-%Y"
TIME_FORMAT_YMD = "%Y-%m-%d"


def get_output_file_path(base_path, template_file):
    report_output_file_path = os.path.join(base_path, 'reports')
    if not os.path.exists(report_output_file_path):
        os.makedirs(report_output_file_path)
    report_template_file = "%s.xlsx" % template_file
    excel_file_path = os.path.join(report_output_file_path, report_template_file)
    if os.path.isfile(excel_file_path):
        os.remove(excel_file_path)
    report_output_file_path = excel_file_path
    return report_output_file_path


def get_dates(start_date):
    try:
        d1 = datetime.strptime(start_date, TIME_FORMAT_MDY)
        d2 = d1 + relativedelta(months=-1)
        delta = d1 - d2  # timedelta
        date_array = []
        for i in range(delta.days):
            date = d1 - timedelta(days=i)
            date_array.append(date.strftime(TIME_FORMAT_YMD))
        end_date = datetime.strptime(date_array[-1], TIME_FORMAT_YMD).strftime(TIME_FORMAT_MDY)
        return date_array, end_date
    except Exception, ex:
        logger.exception(ex)


def validate_date(date_text):
    try:
        datetime.strptime(date_text, '%m-%d-%Y')
        return True
    except ValueError:
        return False


def csv_to_dict(file_path, date_array):
    try:
        logger.info("Inside csv_to_dict function.")
        logger.info("Reading %s file started." % file_path)
        file_object = open(file_path, 'rb')
        reader = csv.reader(file_object)
        excel_dict = {}
        excel_alarm_map = {}
        count = 0
        row_item = []
        for row in reader:
            # print "row data: %s" % row
            alarm_date = datetime.strptime(str(row[0]), '%d-%m-%Y').strftime(TIME_FORMAT_YMD)
            if alarm_date not in date_array:
                continue
            matchObj = re.findall('([^(]+)\s+\(([^)]+)\)', row[5], re.M | re.I)
            if matchObj:
                alarm_name = matchObj[0][0]
                split_str = matchObj[0][1].split(':')
            alarm_id = split_str[1]
            excel_alarm_map[alarm_id] = alarm_name
            is_blocked = 1 if row[6].lower() == "dropped" else 0
            severity = row[7]
            total_events = int(row[8])
            row_item.append([is_blocked, alarm_date, alarm_id, alarm_name, severity, total_events])
            if is_blocked:
                if alarm_id not in excel_dict.keys():
                    excel_dict[alarm_id] = {}
                if severity not in excel_dict[alarm_id].keys():
                    excel_dict[alarm_id][severity] = 0
                excel_dict[alarm_id][severity] += total_events
            #     count += 1
            # if count == 20:
            #     break
        logger.info("Reading %s file completed." % file_path)
        logger.info("Creating dictionary of csv records.")
        return excel_dict, excel_alarm_map, row_item
    except Exception, ex:
        logger.exception(ex)
        raise Exception("Error caught in csv_to_dict function. %s" % ex)


def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def get_nf_severity(severity):
    severity_map = {
        5: "Critical",
        4: "High",
        3: "Medium",
        2: "Low",
        1: "Minor"
    }
    return severity_map.get(severity, severity_map[1])