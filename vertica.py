import vertica_python
import logging
from logger import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class VerticaDatabase(object):

    def __init__(self,
                 database_name='nfdb',
                 server='localhost',
                 port=5433,
                 user='',
                 password=''):
        logger.info("Creating %s instance. User: %s, Database: %s, Server: %s:%s" %
                    (self.__class__.__name__, user, database_name, server, port))

        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.database_name = database_name

        self.con = None

    def __enter__(self):
        if self.con is None:
            self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def connect(self):
        logger.info("DB Connection Initiated")
        conn_info = {
            'host': self.server,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database_name,
            'read_timeout': 600,
            'unicode_error': 'strict',
            'ssl': False
        }

        self.con = vertica_python.connect(**conn_info)
        logger.info("DB Connection Established. User: %s, Database: %s, Server: %s (%s)" %
                    (self.user, self.database_name, self.server, self.port))

    def disconnect(self):
        logger.info("Database connection closed. User: %s, Database: %s, Server: %s (%s)" %
                    (self.user, self.database_name, self.server, self.port))
        if self.con:
            self.con.close()
        self.con = None

    def execute_query(self, query='', select=True):

        if self.con is None or self.con.closed():
            self.connect()

        cursor = self.con.cursor()
        try:
            cursor.execute(query)
            if select:
                return cursor.fetchall()
            else:
                cursor.commit()
        except Exception, ex:
            logger.exception(ex)
        finally:
            cursor.close()

    def get_severity_records(self, customer_id, device_type_ids, start_date, end_date):
        try:
            logger.info("Inside get_severity_records function.")
            query = """SELECT result.is_blocked, result.app_date, result.appalarmid, dm.name, result.nfseverity, result.total_events from
            (SELECT CASE WHEN ia.action_type='b' THEN 1 ELSE 0 END as is_blocked, TO_CHAR(he.apptimestamp, 'YYYY-MM-DD') as app_date, he.appalarmid, nfseverity, count(*) as total_events, he.devicetypeid FROM NFADMIN.HIGHSEVERITYEVENTS he
            LEFT JOIN NFADMIN.IPS_ACTIONS ia on ia.action_name = he.action WHERE he.apptimestamp >= '{0} 00:00:00' AND he.apptimestamp <= '{1} 23:59:59' and he.customerid = {2} GROUP BY he.appalarmid, nfseverity, app_date, he.devicetypeid, is_blocked
            UNION SELECT CASE WHEN ia.action_type='b' THEN 1 ELSE 0 END as is_blocked, TO_CHAR(le.apptimestamp, 'YYYY-MM-DD') as app_date, le.appalarmid, nfseverity, count(*) as total_events, le.devicetypeid FROM NFADMIN.LOWSEVERITYEVENTS le
            LEFT JOIN NFADMIN.IPS_ACTIONS ia on ia.action_name = le.action WHERE le.apptimestamp >= '{0} 00:00:00' AND le.apptimestamp <= '{1} 23:59:59' and le.customerid = {2}  GROUP BY le.appalarmid, nfseverity, app_date, le.devicetypeid, is_blocked) as result
            INNER JOIN NFADMIN.DEVICETYPEALARMS dm on dm.alarmid = result.appalarmid WHERE result.devicetypeid = dm.devicetypeid %s order by result.total_events desc""".format(end_date, start_date, customer_id)
            extra_where = ''
            if device_type_ids:
                extra_where = " and dm.devicetypeid in (%s) " % device_type_ids
            query %= extra_where
            # print query
            logger.info("Query to fetch data: %s" % query)
            rows = self.execute_query(query=query)
            if rows:
                logger.info("Total %s records fetched." % len(rows))
            else:
                logger.info("No records found from database.")
                # raise Exception("Error in fetching records from database.")
            return rows
        except Exception, ex:
            logger.exception(ex)
            raise Exception("Error caught in get_severity_records function. %s" % ex)