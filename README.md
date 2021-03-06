## mod-mongodb-dt-ct-retention-scheduler
This module is used by Scheduler daemon to store downtime and comment records 
associated with services or hosts to a mongoDB database.  

## Basis
Because the update retention file operation is in the scheduler's main loop, in 
order to not delay other operations we make a assumption that if some thing 
error has happened, then we think that the update operation this time has 
failed, we just stop this update operation, log it and wait until next one.

## SetUp
Note: Replica set MongoDB instances is the recommended way to work with.  

### Replica Set  

##### MongoDB Replica Set with three instances
* host1:port1
* host2:port2
* host3:port3  

##### MongoDB Replica Set configuration
* [with authenticate](http://docs.mongodb.org/manual/tutorial/deploy-replica-set-with-auth/)
* [without authenticate](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/)

##### Configuration in mongodb-dt-ct-retention-scheduler.cfg
> module_name     mongodb-dt-ct-retention-scheduler      
> module_type     mongodb_dt_ct_retention_scheduler    
> high_availability     true  
> replica_set       host1:port1, host2:port2, host3:port3    
> url_options   w=1&wtimeoutMS=3000&journal=true&readPreference=secondary&replicaSet=shinken&connectTimeoutMS=3000  
> database     shinken_scheduler_dt_ct_retention  
> username     shinken_scheduler_dt_ct_retention  
> password     shinken_scheduler_dt_ct_retention

### Stand alone

##### MongoDB Stand alone
* host:port

##### Configuration in mongodb-dt-ct-retention-scheduler.cfg
> module_name     mongodb-dt-ct-retention-scheduler  
> module_type     mongodb_dt_ct_retention_scheduler     
> high_availability     false  
> stand_alone   host:port  
> url_options   w=1&wtimeoutMS=3000&journal=true&readPreference=secondary&replicaSet=shinken&connectTimeoutMS=3000    
> database     shinken_scheduler_dt_ct_retention  
> username     shinken_scheduler_dt_ct_retention  
> password     shinken_scheduler_dt_ct_retention  