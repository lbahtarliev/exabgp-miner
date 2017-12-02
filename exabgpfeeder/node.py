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

#from minemeld.ft import base
#from minemeld.ft import actorbase
from minemeld.ft.base import BaseFT
from minemeld.ft.actorbase import ActorBaseFT

VERSION = "0.1"

LOG = logging.getLogger(__name__)

_SYSLOG_LEVELS = {
    'KERN': 0,
    'USER': 1,
    'MAIL': 2,
    'DAEMON': 3,
    'AUTH': 4,
    'SYSLOG': 5,
    'LPR': 6,
    'NEWS': 7,
    'UUCP': 8,
    'CRON': 9,
    'AUTHPRIV': 10,
    'FTP': 11,
    'LOCAL0': 16,
    'LOCAL1': 17,
    'LOCAL2': 18,
    'LOCAL3': 19,
    'LOCAL4': 20,
    'LOCAL5': 21,
    'LOCAL6': 22,
    'LOCAL7': 23
}

_SYSLOG_FACILITIES = {
    'EMERG': 0,
    'ALERT': 1,
    'CRIT': 2,
    'ERR': 3,
    'WARNING': 4,
    'NOTICE': 5,
    'INFO': 6,
    'DEBUG': 7
}

#class Output(ActorBaseFT):
class Output(BaseFT):
    def __init__(self, name, chassis, config):
        super(Output, self).__init__(name, chassis, config)

        self._ls_socket = None

    def configure(self):
        super(Output, self).configure()

        self.exabgp_host = self.config.get('exabgp_host', '127.0.0.1')
        self.exabgp_port = int(self.config.get('exabgp_port', '65002'))

    def connect(self, inputs, output):
        output = False
        super(Output, self).connect(inputs, output)

    def initialize(self):
        pass

    def rebuild(self):
        pass

    def reset(self):
        pass

    def _send_exabgp(self, message, source=None, indicator=None, value=None):
        now = datetime.datetime.now()

        fields = {
            '@timestamp': now.isoformat()+'Z',
            '@version': 1,
            'exabgp_output_node': self.name,
            'message': message
        }

        if indicator is not None:
            fields['@indicator'] = indicator

        if source is not None:
            fields['@origin'] = source

        if value is not None:
            fields.update(value)

        if 'last_seen' in fields:
            last_seen = datetime.datetime.fromtimestamp(
                float(fields['last_seen'])/1000.0
            )
            fields['last_seen'] = last_seen.isoformat()+'Z'

        if 'first_seen' in fields:
            first_seen = datetime.datetime.fromtimestamp(
                float(fields['first_seen'])/1000.0
            )
            fields['first_seen'] = first_seen.isoformat()+'Z'

        try:
            ipaddr = self._genipformat(fields['@indicator'])
            if len(ipaddr) >= 1:
               count = 0
               while count < len(ipaddr):
                     ipcidr = str(ipaddr[count].network) + "/" + str(ipaddr[count].prefixlen)
                     values = { 'command': str(fields['message']) + ' route ' + ipcidr + ' next-hop self' }
                     data = urllib.urlencode(values)
                     req = urllib2.Request('http://' + self.exabgp_host + ':' + str(self.exabgp_port))
                     # req.add_header('Content-Type', 'application/json')
                     req.add_header('Content-Type', 'application/x-www-form-urlencoded')
                     LOG.info("%s: %s - %s of %s", str(fields['message']).upper(), ipcidr, str(count+1), str(len(ipaddr)))
                     response = urllib2.urlopen(req, data)
                     count += 1

            else:
                     LOG.info("Bogon CIDRs found: %s", str(len(ipaddr)))
#                     yield 'ip route 0.0.0.0/32 null0\n'
        except:
            self._ls_socket = None
            raise

        self.statistics['message.sent'] += 1

    @_counting('update.processed')
    def filtered_update(self, source=None, indicator=None, value=None):
        self._send_exabgp(
            'announce',
            source=source,
            indicator=indicator,
            value=value
        )

    @_counting('withdraw.processed')
    def filtered_withdraw(self, source=None, indicator=None, value=None):
        self._send_exabgp(
            'withdraw',
            source=source,
            indicator=indicator,
            value=value
        )

    def length(self, source=None):
        return 0

    def _genipformat(self, indicator=None):
         if '-' in indicator:
            a1, a2 = indicator.split('-', 1)
            return netaddr.iprange_to_cidrs(a1, a2)

         if '/' in indicator:
            ip = netaddr.iprange_to_cidrs(indicator, indicator)
            return ip

         if re.match(r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$", indicator):
            ip = netaddr.iprange_to_cidrs(indicator, indicator)
            return ip
