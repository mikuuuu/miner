import logging


class Logger(object):

    SILENT = False


    def __init__(self, filename):
        self.filename = filename
        logging.basicConfig(filename=self.filename, level=logging.INFO, filemode='w', format='%(message)s')

    def set_silent(self, flag):
        self.SILENT = flag

    def log(self, content):
        if not self.SILENT:
            print content

        logging.info(content)

    def close(self):
        logging.shutdown()



if __name__ == '__main__':
    logger = Logger('log.txt')
    logger.log('HeHe')
    logger.log('^_^')
    logger.close()