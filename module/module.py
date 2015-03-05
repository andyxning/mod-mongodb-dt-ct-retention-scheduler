# -*- coding: utf-8 -*-
# 
# Copyright @ 2015 OPS, Qunar Inc. (qunar.com)
# Author: ning.xie <andy.xning@qunar.com>
# 

import os
import signal
import traceback
from multiprocessing import Process

try:
    from pymongo import MongoReplicaSetClient, MongoClient
    from pymongo.errors import (ConnectionFailure, InvalidURI,
                                DuplicateKeyError, ConfigurationError)
except ImportError:
    raise Exception('Python binding for MongoDB has not been installed. '
                    'Please install "pymongo" first')

from shinken.basemodule import BaseModule
from shinken.log import logger
from shinken.util import to_bool


properties = {
              'daemons': ['scheduler'],
              'type': 'mongodb-dt-ct-retention-scheduler',
              'external': False
              }


# called by the plugin manager to get a mongodb_dt_ct_scheduler instance
def get_instance(mod_conf):
    logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] Get a Scheduler module %s' 
                % mod_conf.get_name())
    instance = MongodbDtCtRetentionScheduler(mod_conf)
    return instance


# Main class
class MongodbDtCtRetentionScheduler(BaseModule):
    
    def __init__(self, mod_conf):
        BaseModule.__init__(self, mod_conf)
        self._parse_conf(mod_conf)
        
        self.conn = None
        self.task = None
        
    def _parse_conf(self, mod_conf):
        self.high_availability = to_bool(getattr(mod_conf,
                                                 'high_availability', 'false'))
        if not self.high_availability:
            self.stand_alone = getattr(mod_conf, 'stand_alone', '')
            if not self.stand_alone:
                logger.error('[Mongodb-Dt-Ct-Retention-Scheduler] Mongodb is '
                             'configured with high availability be false but '
                             'stand_alone is not configured')
                raise Exception('[Mongodb-Dt-Ct-Retention-Scheduler] '
                                'Configuration Error')
        else:
            replica_set_str = getattr(mod_conf, 'replica_set', '')
            self._set_replica_set(replica_set_str)
            
        self.database = getattr(mod_conf,
                                'database', 'shinken_dt_ct_retention_scheduler')
        self.username = getattr(mod_conf,
                                'username', 'shinken_dt_ct_retention_scheduler')
        self.password = getattr(mod_conf,
                                'password', 'shinken_dt_ct_retention_scheduler')
        self.url_options = getattr(mod_conf, 'url_options', '')
        
    def _set_replica_set(self, replica_set_str):
        raw_members = replica_set_str.split(',')
        members = []
        for member in raw_members:
            members.append(member.strip())
        self.replica_set = members        
        
    def _set_mongodb_url(self):
        scheme = 'mongodb://'
        db_and_options = '/%s?%s' % (self.database, self.url_options) 
        credential = ':'.join((self.username, '%s@' % self.password))
        if not self.high_availability:
            address = self.stand_alone
            mongodb_url = ''.join((scheme, credential, address, db_and_options))
        else:
            address = ','.join(self.replica_set)
            mongodb_url = ''.join((scheme, credential, address, db_and_options))
        self.mongodb_url = mongodb_url
        
    # Called by Scheduler to do init work
    def init(self):
        logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] Initialization of '
                    'mongodb_retention_scheduler module')
        self._set_mongodb_url()
        logger.debug('[Mongodb-Dt-Ct-Retention-Scheduler] Mongodb connect url: '
                     '%s' % self.mongodb_url)
    
    def _init(self):   
        self._do_stop()
        
        try:
            if not self.high_availability:
                self.conn = MongoClient(self.mongodb_url)
            else:
                self.conn = MongoReplicaSetClient(self.mongodb_url)
        except ConnectionFailure:
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] Can not make '
                        'connection with MongoDB')
            raise
            
        except (InvalidURI, ConfigurationError):
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] Mongodb '
                        'connect url error')
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] Mongodb '
                        'connect url: %s' % self.mongodb_url)
            raise
        
        logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] Got a mongodb '
                    ' connection.') 
        self._get_collections()
        
    def _get_collections(self):
        db = self.conn[self.database]
        self.services = db['services']
        self.downtimes = db['downtimes']
        self.comments = db['comments']
    
    def _do_stop(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # invoked by the Scheduler daemon
    # We must not do any thing that will last for a long time. It will delay 
    # other operation in the Scheduler daemon's main event loop.
    def hook_save_retention(self, daemon):
        retention = daemon.get_retention_data()
        if self.task and self.task.is_alive():
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] Previous task has '
                        'not been accomplished but this should not happen. '
                        'We should stop it.')
            os.kill(self.task.pid, signal.SIGKILL)
        self.task = None
        # must be args=(retention,) not args=(retention)
        self.task = Process(target=self._hook_save_retention, args=(retention,))
        self.task.daemon = True
        self.task.start()
        logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] New update begins.')

    def _hook_save_retention(self, retention):
        self.set_proctitle(self.name)
        try:
            self._init()
        except Exception:
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] '
                        'Update ends error.')
            return
        dts_and_comments = self._get_dt_and_comment(retention)
        self._update_dt_and_comment_retention(dts_and_comments)
        self._do_stop() 
    
    
    def _get_dt_and_comment(self, retention):
        services = retention['services']
        dts_and_comments = []
        for (host, service) in services:
            comments = services[(host, service)].get('comments', [])
            comments = self._get_elements(comments)
            downtimes = services[(host, service)].get('downtimes', [])
            downtimes = self._get_elements(downtimes)
            dts_and_comments.append(((host, service), comments, downtimes))
        return dts_and_comments
            
    # convert object to dict
    def _get_elements(self, elements):
        elts = []
        for elt in elements:
            item = {}
            cls = elt.__class__
            item['_id'] = '%s-%s-%s-%s' % (elt.ref.host.host_name,
                                           elt.ref.service_description,
                                           elt.id, elt.entry_time)
            for prop in cls.properties:
                if hasattr(elt, prop):
                    item[prop] = getattr(elt, prop)
            elts.append(item)
        return elts                  
    
    def _update_dt_and_comment_retention(self, dts_and_comments):
        logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] '
                    'Downtime and Comment update starts.')
        try:
            for elt in dts_and_comments:
                service_identity, comments, downtimes = elt
                _id = ','.join(service_identity)
                cursor = self.services.find({'_id': _id})
                if not cursor.count():
                    service_info = {'_id': _id,
                                    'comment_ids': [],
                                    'downtime_ids': []
                                    }
                    self.services.insert(service_info)
                for comment in comments:
                    try:
                        self.comments.insert(comment)
                    except DuplicateKeyError:
                        pass
                    else:
                        cursor = self.services.find({'_id': _id})
                        comment_ids = cursor[0].get('comment_ids')
                        comment_ids.append(comment.get('_id'))
                        self.services.update({'_id': _id},
                                             {'$set': {'comment_ids': comment_ids}})
                for downtime in downtimes:
                    try:
                        self.downtimes.insert(downtime)
                    except DuplicateKeyError:
                        pass
                    else:
                        self.services.find({'_id': _id})
                        downtime_ids = cursor[0].get('downtime_ids')
                        downtime_ids.append(downtime.get('_id'))
                        self.services.update({'_id': _id},
                                             {'$set': {'downtime_ids': downtime_ids}})
            
            logger.info('[Mongodb-Dt-Ct-Retention-Scheduler] '
                        'Downtime and Comment update ends successfully.')
        except Exception:
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] '
                        'Downtime and Comment update error.')
            logger.warn('[Mongodb-Dt-Ct-Retention-Scheduler] '
                        '%s' % traceback.format_exc())
            
