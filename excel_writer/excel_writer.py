import string
import logging
import collections
import xlsxwriter
from datetime import datetime
from logger import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class ExcelWriter(object):
    def __init__(self, template_name):
        logger.info("Creating instance of %s." % self.__class__.__name__)
        self.excel_file_path = template_name
        self.workbook_instance = xlsxwriter.Workbook(self.excel_file_path)
        logger.info("Initializing Workbook instance.")

    def close(self):
        if self.workbook_instance is not None:
            logger.info("Workbook instance closed.")
            self.workbook_instance.close()

    def write_to_document_file(self, sheet_name, date_str):
        try:
            logger.info("Inside write_to_document_file function.")
            worksheet = self.workbook_instance.add_worksheet(sheet_name)
            logger.info("Opening %s file for writing %s sheet." % (self.excel_file_path, sheet_name))
            format1 = self.workbook_instance.add_format({'font_size': 15, 'bold': True})
            format2 = self.workbook_instance.add_format({'font_color': 'blue'})
            format3 = self.workbook_instance.add_format({'border': 1, 'font_size': 15, 'bold': True})
            format4 = self.workbook_instance.add_format({'border': 1})
            format5 = self.workbook_instance.add_format({'border': 1, 'align': 'right'})
            alphabets = list(string.ascii_uppercase)
            set_columns_size = [50, 30, 20, 25]
            for i in range(len(set_columns_size)):
                worksheet.set_column('%s:%s' % (alphabets[i],alphabets[i]), set_columns_size[i])

            worksheet.write("A4", "In Confidence", format1)
            worksheet.write("A5", "Uncontrolled if Printed", format1)
            temp_data = ["Documents that do not require approval by BT and / or P&G to be adopted",
                         "(Reports, meeting minutes, project schedules)",
                         "If you can answer the 5 Ws in your document - you have covered good documentation practices: "
                         "Who, What, Where, Why, When?"]
            count = 10
            worksheet.write("%s%s" % (alphabets[0], count), "Standard Document:", format1)
            for i in range(len(temp_data)):
                count += 1
                worksheet.write("%s%s" % (alphabets[0], count), temp_data[i], format2)

            data = [["Document Owner(s)", "Title", "Business Unit"],
                    ["", "Preventative Attack & Uptime Report", "Compliance"]]
            data_formats = [format3, format4]
            row = 20
            worksheet.write("%s%s" % (alphabets[0], row), "Prepared by:", format1)
            for i in range(len(data)):
                row += 1
                for j in range(len(data[i])):
                    worksheet.write('%s%s' % (alphabets[j], row), data[i][j], data_formats[i])
            document_data = [["Version", "Date: DD-MMM-YYYY", "Author", "Change Description"],
                             ["Original", "", "", ""], [1.1, "", "", ""], [1.2, "", "", ""]]
            row_count = 25
            worksheet.write("%s%s" % (alphabets[0], row_count), "Document History", format1)
            document_data_format = [format3, format4, format4, format4]
            for i in range(len(document_data)):
                row_count += 1
                for j in range(len(document_data[i])):
                    worksheet.write('%s%s' % (alphabets[j], row_count), document_data[i][j], document_data_format[i])
            worksheet.write("A32", "DATE:", format3)
            worksheet.write("A33", "DATE: DD-MMM-YYYY", format3)
            worksheet.write("A34", datetime.strptime(date_str, "%m-%d-%Y").strftime("%d-%b-%Y"), format5)
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error in write_to_document_file function. %s" % ex)

    def write_data_worksheet(self, sheet_name, data, top_alarms):
        try:
            logger.info("Inside write_data_worksheet function.")
            ordered_data = collections.OrderedDict(sorted(data.items()))
            # print "sorted dictionary: %s" % ordered_data
            work_sheet = self.workbook_instance.add_worksheet(sheet_name)
            bold_format = self.workbook_instance.add_format({'bold': True})
            logger.info("Opening %s file for writing %s sheet." % (self.excel_file_path, sheet_name))
            columns = ["A", "C", "D", "E", "F", "G", "J", "K"]
            column_header = ["DATE"]
            for alarm_id in top_alarms:
                column_header.extend([top_alarms[alarm_id]])
            column_header.extend(["Blocked (All)", "Unblocked Events"])
            # adding header
            for i in range(len(columns)):
                work_sheet.write("%s%s" % (columns[i], 1), column_header[i], bold_format)
            # adding data
            row_count = 2
            for date in ordered_data:
                work_sheet.write("A%s" % row_count, date)
                work_sheet.write("J%s" % row_count, ordered_data[date]["blocked"])
                work_sheet.write("K%s" % row_count, ordered_data[date]["unblocked"])
                for i, alarm_id in enumerate(top_alarms):
                    work_sheet.write("%s%s" % (columns[i + 1], row_count), ordered_data[date]["alarms"][alarm_id])
                row_count += 1
            logger.info("Writing records to %s file completed successfully." % self.excel_file_path)
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error in write_data_worksheet function. %s" % ex)

    def draw_main_worksheet_chart(self, worksheet, cell):
        try:
            logger.info("Inside draw_main_worksheet_chart function.")
            chart = self.workbook_instance.add_chart({'type': 'line'})
            chart.set_size({'width': 650, 'height': 500})
            chart.add_series({'name': 'DATA!$J$1', 'values': '=DATA!$J$2:$J$33', 'categories': '=DATA!$A$2:$B$33'})
            chart.add_series({'name': 'DATA!$K$1', 'values': '=DATA!$K$2:$K$33', 'categories': '=DATA!$A$2:$B$33'})
            worksheet.insert_chart(cell, chart)
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error in draw_main_worksheet_chart function. %s" % ex)

    def get_cell_format(self, color_name):
        cell_format = self.workbook_instance.add_format()
        cell_format.set_pattern()
        cell_format.set_bg_color(color_name)
        return cell_format

    def write_main_worksheet(self, sheet_name, data, start_date, end_date):
        try:
            logger.info("Inside write_main_worksheet function.")
            logger.info("Opening %s file for writing %s sheet." % (self.excel_file_path, sheet_name))
            work_sheet = self.workbook_instance.add_worksheet(sheet_name)
            bold_format = self.workbook_instance.add_format({'bold': True, 'border': 1})
            border_format = self.workbook_instance.add_format({'border': 1})
            font_format = self.workbook_instance.add_format({'font_size': 15, 'bold': True})
            italic_format = self.workbook_instance.add_format({'italic': True})
            work_sheet.write("A1", "Preventative Attack Report for", font_format)
            work_sheet.write("D1", "For period: %s TO %s" % (datetime.strptime(end_date, "%m-%d-%Y").strftime("%d-%b-%Y"),
                                datetime.strptime(start_date, "%m-%d-%Y").strftime("%d-%b-%Y")))
            work_sheet.write("A3", "Prepared For Procter & Gamble", italic_format)
            # table coding starts
            table_header = ["Action", "Count", "Percentage"]
            alphabets = list(string.ascii_uppercase)
            for i in range(len(table_header)):
                work_sheet.write('%s5' % alphabets[i], table_header[i], bold_format)
            work_sheet.write('%s6' % alphabets[0], "Blocked", border_format)
            work_sheet.write('%s6' % alphabets[1], "=SUM(B11:B500)", border_format)
            work_sheet.write('%s7' % alphabets[0], "Total", border_format)
            work_sheet.write('%s7' % alphabets[1], "=SUM(DATA!J2:K50)", border_format)
            work_sheet.write('%s7' % alphabets[2], 100, border_format)
            work_sheet.write('%s6' % alphabets[2], "=B6/B7", border_format)

            # table coding ends
            headers = ["Blocked Attacks", "Count", "Severity", "Alert Name"]
            work_sheet.set_column('%s:%s' % (alphabets[3], alphabets[3]), 50)
            row_num = 10
            for i in range(len(headers)):
                work_sheet.write('%s%s' % (alphabets[i], row_num), headers[i], bold_format)
            row_count = 11
            for count, row in enumerate(data):
                alert_name = row[1]
                severity = row[2]
                event_count = row[3]
                row_item = [event_count, severity, alert_name]
                work_sheet.write('%s%s' % (alphabets[0], row_count), count+1)
                for i in range(len(row_item)):
                    if severity.lower() == "high":
                        work_sheet.write('%s%s' % (alphabets[i+1], row_count), row_item[i],
                                         self.get_cell_format('#F53B13'))
                    elif severity.lower() == "low":
                        work_sheet.write('%s%s' % (alphabets[i+1], row_count), row_item[i],
                                         self.get_cell_format('#91F617'))
                    elif severity.lower() == "medium":
                        work_sheet.write('%s%s' % (alphabets[i + 1], row_count),
                                         row_item[i], self.get_cell_format('#F0FF05'))
                    else:
                        work_sheet.write('%s%s' % (alphabets[i + 1], row_count), row_item[i])
                row_count += 1
            logger.info("Writing records to %s file completed successfully." % self.excel_file_path)
            self.draw_main_worksheet_chart(work_sheet, "E%s" % row_num)
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error in write_main_worksheet function. %s" % ex)

    def draw_top5_charts(self, sheet_name):
        try:
            logger.info("Inside draw_top5_charts function.")
            work_sheet = self.workbook_instance.add_worksheet(sheet_name)
            columns = ["C", "D", "E", "F", "G"]
            chart_columns = ["A2", "J2", "A20", "J20", "D40"]
            for i in range(5):
                chart = self.workbook_instance.add_chart({'type': 'line'})
                chart.set_size({'width': 520, 'height': 350})
                chart.add_series({'name': 'DATA!$%s$1' % columns[i],
                                  'values': '=DATA!$%s$2:$%s$33' % (columns[i], columns[i]),
                                  'categories': '=DATA!$A$2:$A$33'})
                work_sheet.insert_chart(chart_columns[i], chart)
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error in draw_top5_charts function. %s" % ex)
