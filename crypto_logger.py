import logging
import logging.handlers
from logging import Handler, Formatter
from notifications_handler import NotificationHandler

class Crypto_Logger():

    logger = None
    notification_handler = None

    def __init__(self):
        # Logger setup
        self.logger = logging.getLogger('crypto_trader_logger')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler('crypto_trading.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # logging to console
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        # notification handler
        self.notification_handler = NotificationHandler()

    def log(self, message, level = 'info', notification = True):
        
        if 'info' == level:
            self.logger.info(message)

        if notification and self.notification_handler.enabled:
            self.notification_handler.send_notification(message)

Logger = Crypto_Logger()