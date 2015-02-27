# coding=utf-8

"""
[Fluentd](http://fluentd.org)  is an open source data collector for unified
logging layer. Fluentd allows you to unify data collection and consumption
for a better use and understanding

This handler was based on LibratoHandler

#### Dependencies

 * [fluent-logger-python](https://github.com/fluent/fluent-logger-python)

#### Configuration

Enable this handler

 * handlers = diamond.handler.fluentdhandler.FluentdHandler,

 * prefix_tag = [optional | 'diamond'] Prefix for fluentd tag
 * queue_max_size = [optional | 300] max measurements to queue before submitting
 * queue_max_interval [optional | 60] @max seconds to wait before submitting
     For best behavior, be sure your highest collector poll interval is lower
     than or equal to the queue_max_interval setting.

 * include_filters = [optional | '^.*'] A list of regex patterns.
     Only measurements whose path matches a filter will be submitted.
     Useful for limiting usage to *only* desired measurements, e.g.
       include_filters = "^diskspace\..*\.byte_avail$", "^loadavg\.01"
       include_filters = "^sockets\.",
                                     ^ note trailing comma to indicate a list

"""

from Handler import Handler
import logging
import time
import re
from fluent import sender
from fluent import event

class FluentdHandler(Handler):

    def __init__(self, config=None):
        """
        Create a new instance of the FluentdHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("Initialized Fluentd handler.")

        if sender is None:
            logging.error("Failed to load fluent-logger-python module")
            return

        # Initialize Options
        self.queue = []
        self.queue_max_size = int(self.config['queue_max_size'])
        self.queue_max_interval = int(self.config['queue_max_interval'])
        self.queue_max_timestamp = int(time.time() + self.queue_max_interval)
        self.current_n_measurements = 0

        self.log.debug("Connecting to fluentd host %s" % (self.config['host']))

        sender.setup(self.config['prefix_tag'], host=self.config['host'], port=int(self.config['port']))

        # If a user leaves off the ending comma, cast to a array for them
        include_filters = self.config['include_filters']
        if isinstance(include_filters, basestring):
            include_filters = [include_filters]

        self.include_reg = re.compile(r'(?:%s)' % '|'.join(include_filters))

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(FluentdHandler, self).get_default_config_help()

        config.update({
            'prefix_tag': 'Prefix tag for metrics',
            'host': 'fluentd host, default: 127.0.0.1',
            'port': 'fluentd port, default: 24224',
            'queue_max_size': '',
            'queue_max_interval': '',
            'include_filters': '',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(FluentdHandler, self).get_default_config()

        config.update({
            'prefix_tag': 'diamond',
            'host': '127.0.0.1',
            'port': 24224,
            'queue_max_size': 1,
            'queue_max_interval': 60,
            'include_filters': ['^.*'],
        })

        return config

    def process(self, metric):
        """
        Process a metric by sending it to Fluentd
        """
        path = metric.getCollectorPath()
        path += '.'
        path += metric.getMetricPath()

        if self.include_reg.match(path):
            if metric.metric_type == 'GAUGE':
                m_type = 'gauge'
            else:
                m_type = 'counter'
            self.queue.append({
                            'metric': path,                # name
                            'value': float(metric.value),  # value
                            'type': m_type,
                            'host': metric.host,
                            'timestamp': metric.timestamp})
            self.current_n_measurements += 1
        else:
            self.log.debug("FluentdHandler: Skip %s, no include_filters match",
                           path)

        if (self.current_n_measurements >= self.queue_max_size or
                time.time() >= self.queue_max_timestamp):
            self.log.debug("FluentdHandler: Sending batch size: %d",
                           self.current_n_measurements)
            self._send()

    def flush(self):
        """Flush metrics in queue"""
        self._send()

    def _send(self):
        """
        Send data to Fluentd.
        """

        while self.queue:
            item = self.queue.pop()
            metric = item['metric']
            del item['metric']
            # send event to fluentd, with 'app.follow' tag
            event.Event(metric, item)
            #self.log.debug("sending mtric %s" % (metric))

        self.queue_max_timestamp = int(time.time() + self.queue_max_interval)
        self.current_n_measurements = 0
