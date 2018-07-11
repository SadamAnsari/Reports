import os
import logging
from logging.handlers import RotatingFileHandler

LOGGER_NAME = "BT_REPORT"


class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    pass
    """def doRollover(self):
        self.stream.close()

        dfn = self.baseFilename + "." + time.strftime("%Y%m%d%H%M%S")
        if os.path.exists(dfn):
            os.remove(dfn)
        os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            # find the oldest log file and delete it
            s = glob.glob(self.baseFilename + ".20*")
            if len(s) > self.backupCount:
                s.sort()
                os.remove(s[0])
        #print "%s -> %s" % (self.baseFilename, dfn)
        if self.encoding:
            self.stream = codecs.open(self.baseFilename, 'w', self.encoding)
        else:
            self.stream = open(self.baseFilename, 'w')
        if os.path.exists(dfn + ".zip"):
            os.remove(dfn + ".zip")
        file = zipfile.ZipFile(dfn + ".zip", "w")
        file.write(dfn, os.path.basename(dfn), zipfile.ZIP_DEFLATED)
        file.close()
        os.remove(dfn)
        """


def get_loglevel(loglevel_str):
    loglevel_str = loglevel_str.lower()
    loglevel = {"debug": logging.DEBUG,
                "info": logging.INFO,
                "warning": logging.WARNING,
                "warn": logging.WARNING,
                "error": logging.ERROR,
                "fatal": logging.FATAL,
                "critical": logging.CRITICAL
                }
    if loglevel_str not in loglevel.keys():
        loglevel_str = "warning"

    return loglevel.get(loglevel_str)


def setup_logging(logger_name=LOGGER_NAME, logdir="logs", logfile="notifier.log",
                  scrnlog=False, txtlog=True, loglevel=logging.DEBUG):
    logdir = os.path.abspath(logdir)
    print logdir
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    log = logging.getLogger(logger_name)
    log.setLevel(loglevel)

    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s %(process)d %(filename)s:%(lineno)d  :: %(message)s")

    if txtlog:
        txt_handler = RotatingFileHandler(os.path.join(logdir, logfile), mode='a',
                                          maxBytes=1024 * 1024 * 3, backupCount=20)
        # txt_handler.doRollover()
        txt_handler.setFormatter(log_formatter)
        log.addHandler(txt_handler)
        log.info("Logger initialized.")

    if scrnlog:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        log.addHandler(console_handler)
