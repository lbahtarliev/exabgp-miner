#  Copyright 2017 AKAT Technologies OOD

from __future__ import absolute_import

import logging
import ujson
import datetime
import socket
import re
import netaddr
import urllib
import urllib2

from minemeld.ft.base import _counting
from minemeld.ft import table
from minemeld.ft.utils import utc_millisec
#from minemeld.ft import actorbase
from minemeld.ft.base import BaseFT
from minemeld.ft.actorbase import ActorBaseFT

VERSION = "0.8"

LOG = logging.getLogger(__name__)

class Output(ActorBaseFT):
    def __init__(self, name, chassis, config):
        self.locals = {
            'version': VERSION
        }

        self._actor = None

        super(Output, self).__init__(name, chassis, config)

    def configure(self):
        super(Output, self).configure()

        self.queue_maxsize = int(self.config.get('queue_maxsize', 100000))
        if self.queue_maxsize == 0:
            self.queue_maxsize = None

        self.exabgp_host = self.config.get('exabgp_host', '127.0.0.1')
        self.exabgp_port = int(self.config.get('exabgp_port', '65002'))
        self.age_out = self.config.get('age_out', 3600)
        self.age_out_interval = self.config.get('age_out_interval', 86400)

    def connect(self, inputs, output):
        output = False
        super(Output, self).connect(inputs, output)

    def _initialize_table(self, truncate=False):
        self.table = table.Table(self.name, truncate=truncate)
        self.table.create_index('_age_out')

    def initialize(self):
        self._initialize_table()

    def rebuild(self):
        self.rebuild_flag = True
        self._initialize_table(truncate=True)

    def reset(self):
        self._initialize_table(truncate=True)

    def _eval_send_exabgp(self, message, source=None, indicator=None, value=None):
        indicators = [indicator]
        if '-' in indicator:
           a1, a2 = indicator.split('-', 1)
           indicators = map(str, netaddr.iprange_to_cidrs(a1, a2))
        # Already in our format? Just convert it to IPNetwork object
        elif '/' in indicator:
           indicators = map(str, netaddr.iprange_to_cidrs(indicator, indicator))
        # Single host one per line
        elif re.match(r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$", indicator):
           indicators = map(str, netaddr.iprange_to_cidrs(indicator, indicator))

        try:
          for i in indicators:
            value['__indicator'] = i
            now = utc_millisec()
            age_out = now+self.age_out*1000
            value['_age_out'] = age_out
            values = { 'command': str(fields['message']) + ' route ' + i + ' next-hop self' }
            data = urllib.urlencode(values)
            req = urllib2.Request('http://' + self.exabgp_host + ':' + str(self.exabgp_port))
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            # req.add_header('Content-Type', 'application/json')
            response = urllib2.urlopen(req, data)
            #LOG.info("%s: %s - %s of %s", str(fields['message']).upper(), ipcidr, str(count+1), str(len(ipaddr)))
            self.statistics['bgp-messages.sent'] += 1

        except:
          pass


    @_counting('update.processed')
    def filtered_update(self, source=None, indicator=None, value=None):
        self.statistics['added'] += 1
        self.table.put(str(i), value)
        self._eval_send_exabgp('announce', source=source, indicator=indicator, value=value)


    @_counting('withdraw.processed')
    def filtered_withdraw(self, source=None, indicator=None, value=None):
        self.statistics['removed'] += 1
        self.table.delete(str(i))
        self._eval_send_exabgp('withdraw', source=source, indicator=indicator, value=value)


    def mgmtbus_status(self):
        result = super(Output, self).mgmtbus_status()
        return result

    def length(self, source=None):
        return self.table.num_indicators
        #return self.length()

    def start(self):
        super(Output, self).start()

    def stop(self):
        super(Output, self).stop()
