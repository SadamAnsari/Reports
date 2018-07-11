import logging
import operator
from utility import *
from logger import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


def get_date_wise_result(results, date_wise_result, top_5, data_from):
    try:
        logger.info("Inside get_date_wise_result function.")
        logger.info("Iterating through the %s records." % data_from)
        for row_item in results:
            # print row_item
            is_blocked = row_item[0]
            alarm_date = str(row_item[1])
            alarm_id = row_item[2]
            total_events = row_item[5]
            if alarm_date not in date_wise_result.keys():
                continue
            if alarm_id in top_5.keys() and is_blocked:
                date_wise_result[alarm_date]["alarms"][alarm_id] += int(total_events)

            if is_blocked:
                date_wise_result[alarm_date]["blocked"] += int(total_events)
            else:
                date_wise_result[alarm_date]["unblocked"] += int(total_events)
        logger.info("Getting date wise results into dictionary from %s records." % data_from)
        return date_wise_result
    except Exception, ex:
        logger.exception(ex)
        raise Exception(" Error caught in get_date_wise_result function. %s" % ex)


def get_main_result_dict(results):
    try:
        logger.info("Preparing result dictionary from database results.")
        alarm_map = {}
        main_result = {}
        for row_item in results:
            is_blocked = row_item[0]
            alarm_id = row_item[2]
            alarm_map[alarm_id] = row_item[3]
            severity = get_nf_severity(row_item[4])
            total_events = row_item[5]
            if is_blocked == 0:
                if alarm_id not in main_result.keys():
                    main_result[alarm_id] = {}
                if severity not in main_result[alarm_id].keys():
                    main_result[alarm_id][severity] = 0
                main_result[alarm_id][severity] += total_events
        return main_result, alarm_map
    except Exception, ex:
        logger.exception("Error caught in get_main_result_dict function")
        raise


def convert_dict_to_list(merge_results, alarm_map, excel_alarm_map):
    try:
        logger.info("Inside convert_dict_to_list function.")
        logger.info("Creating list from merged dictionary.")
        rows = []
        for key, value in merge_results.iteritems():
            row_item = []
            if key in alarm_map.keys():
                alarm_name = alarm_map[key]
            if key in excel_alarm_map.keys():
                alarm_name = excel_alarm_map[key]
            row_item.extend([key, alarm_name])
            for k in value:
                row_item.extend([k, value[k]])
            rows.append(row_item)
        logger.info("List creation completed from merged dictionary.")
        return rows
    except Exception, ex:
        logger.exception(ex)
        raise Exception("Error in convert_dict_to_list function. %s" % ex)


def fetch_records(db_instance, customer_id, device_type_ids, start_date, end_date, date_range, csv_file_path):
    try:
        logger.info("Starting fetch_records. start_date:%s, end_date: %s" % (start_date, end_date))
        results = db_instance.get_severity_records(customer_id, device_type_ids, start_date, end_date)
        main_result, alarm_map = get_main_result_dict(results=results)
        excel_result, excel_alarm_map, excel_rows = csv_to_dict(file_path=csv_file_path, date_array=date_range)
        logger.info("Merging of %d database records with %d csv records." % (len(main_result), len(excel_result)))
        merge_two_dict = merge(dict(main_result), excel_result)
        converted_list = convert_dict_to_list(
            merge_results=merge_two_dict,
            alarm_map=alarm_map,
            excel_alarm_map=excel_alarm_map
        )
        sorted_rows = sorted(converted_list, key=operator.itemgetter(3), reverse=True)
        top_5_alarms = get_top5_records(sorted_rows)
        date_wise_result = {}
        for date in date_range:
            date_wise_result[date] = {
                "alarms": {},
                "blocked": 0,
                "unblocked": 0
            }
            for item in top_5_alarms.keys():
                date_wise_result[date]["alarms"][item] = 0

        db_datewise_result = get_date_wise_result(results=results, date_wise_result=date_wise_result,
                                                  top_5=top_5_alarms, data_from="database")
        excel_datewise_result = get_date_wise_result(results=excel_rows, date_wise_result=db_datewise_result,
                                                     top_5=top_5_alarms, data_from="csv")
        return excel_datewise_result, converted_list, top_5_alarms
    except Exception, ex:
        logger.exception(ex)
        raise Exception("Error caught in fetch_records function.")


def get_top5_records(sorted_records):
    logger.info("Inside get_top5_records function.")
    top_5_list = {}
    for item in sorted_records:
        top_5_list[item[0]] = item[1]
        if len(top_5_list.keys()) == 5:
            break
    logger.info("Top Alarms Information:: %s" % (", ".join(top_5_list.values())))
    return top_5_list
