"use strict"
var setter = require('couch_set_state')
var checker = require('couch_check_state')
var getter = require('couchdb_get_views')
var putter = require('couchdb_put_doc')
var _ = require('lodash')
var cellids = require('../resources/cellids.json')
var queue = require('d3-queue').queue
var check_if_year_db = require('./couchdb_year_check.js')
/**
 * filter_out_done
 *
 * arguments:
 *
 * @param {Object} task
 * @param {Object} task.cell_id - the cell id to mark
 * @param {Object} task.year - the year
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_state_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_state_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {Function} cb: a callback, will get true for files that are NOT done,
 *     or false for files that are done
 *
 * The false for done files will filter them out of the resulting list
 *
 * Use this in async.filter, as in
 * async.filter(jobs,filter_done,function(todo_jobs){
 *   // todo jobs has only the task items that have not yet been done
 * })
 *
 * @returns {} null
 */
function filter_out_done(task,cb){
    // go look up in couchdb
    var opts = _.assign({}
                       ,task.options.couchdb
                       ,{doc:task.cell_id
                        ,year:task.year
                        ,state:'accumulate'})

    if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_state_db


    checker(opts
           ,function(err,state){
                var result = false
               if(err){
                   console.log(err)
                   return cb(err)
               }
                if(state !== undefined){
                    task.state=state
                }

                if(!state ||
                   state.length<3 ){
                    result=true
                }

                task.todo=result
                return cb(null,task)
            })
    return null
}


//
/**
 * in_process
 *
 *  mark the data as being "in process".  The result will be to set
 *  "accumulate" state to equal the list
 *  ['_county','_airbasin','_airdistrict']
 *
 * @param {Object} task
 * @param {Object} task.cell_id - the cell id to mark
 * @param {Object} task.year - the year
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_state_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_state_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {Function} cb
 * @returns {}
 */
function in_process(task,cb){
    var opts = _.assign({}
                       ,task.options.couchdb
                       ,{doc:task.cell_id
                        ,year:task.year
                        ,state:'accumulate'
                        ,'value':['_county','_airbasin','_airdistrict']})
    if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_state_db

    setter(opts
          ,function(err){
               if(err){
                   console.log(err)
                   throw new Error(err)
               }
               return cb(null,task)
           }
          )
    return null
}



/**
 * mark_done
 *  mark the accumulate field as being "done"
 * @param {Object} task
 * @param {Object} task.cell_id - the cell id to mark
 * @param {Object} task.year - the year
 * @param {Object} task.finished_areas - the value of "accumulate" to set in the tracking doc
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_state_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_state_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {Function} cb
 * @returns {}
 */
function mark_done(task,cb){
    var opts = _.assign({}
                       ,task.options.couchdb
                       ,{doc:task.cell_id
                        ,year:task.year
                        ,state:'accumulate'
                        ,'value':task.finished_areas})
    if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_state_db
    setter(opts
          ,function(err,state){
               if(err) throw new Error(err)
               cb(null,task)
           }
          )
    return null
}

/**
 * get_hpms_fractions_one_hour
 * @param {Object} task
 * @param {string} task.ts - The exact hour to fetch
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_hpms_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_hpms_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {Function} cb
 * @returns {}
 * @throws {}
 */
function get_hpms_fractions_one_hour(task,cb){
    // go to couchdb, get fractions for all cells for this hour
    var view = "_design/calvad/_view/ts_grid"
    var date = task.ts
    var _task = {year:task.year,
                 ts:task.ts,
                 options:{}}
    _task.options.couchdb = _.assign({},task.options.couchdb) // local copy

    if(_task.options.couchdb.db === undefined){
        _task.options.couchdb.db = _task.options.couchdb.grid_merge_couchdbquery_hpms_db
    }
    check_if_year_db(_task,function(e,yropts){
        var opts = _.assign({}
                            ,yropts.options.couchdb
                            ,{'view':view
                              ,'include_docs':false
                              ,'reduce':false
                              ,'startkey':[date]
                              ,'endkey':[date+'A'] // some random character
                             })


        getter(opts
               ,function(err,docs){
                   var aadt_element = 'aadt_frac'
                   var aadt_fractions_names = ['n','hh','nhh']
                   task.hpms_fractions = {}
                   task.hpms_fractions_cells = 0
                   if(err){
                       console.log(err)
                       throw new Error(err)
                   }

                   if(docs !== undefined
                      && docs.rows !== undefined
                      && docs.rows.length>0){
                       //console.log(docs.rows.length + ' rows from hpms_fractions query')
                       //console.log(docs.rows[0])

                       // currently using a view, so this is already fixed in view
                       //
                       // temporary hack.  until 2012 is fixed, need to do this
                       // if(docs.rows[0].doc.aadt_frac === undefined){
                       //     if( docs.rows[0].doc.aadt !== undefined){
                       //         aadt_element = 'aadt'
                       //     }else{
                       //         // oops
                       //         throw new Error('couchdb docs missing both aadt and aadt_frac')
                       //     }
                       // }

                       _.forEach(docs.rows,function(row){
                           var geom_id = row.key[1]
                           task.hpms_fractions[geom_id]={}
                           task.hpms_fractions_cells++
                           _.each(aadt_fractions_names,function(k,i){
                               var v = row.value[i]
                               if(!v) v = 0 // force null, undefined to be zero
                               task.hpms_fractions[geom_id][k]=+v
                               return null
                           })
                           return null
                       })
                   }else{
                       //console.log('hpms_fractions nothing')
                       if(docs.error !== undefined){
                           throw new Error(docs)
                       }
                   }
                   return cb(null,task)
               })
        return null
    })
    return null
}

/**
 * get_hpms_scales
 * go to couchdb, get summed fractions and day count for all cells for this year
 *
 * @param {Object} task
 * @param {Object} task.options - the various db access options in the task
 * @param {Object} task.options.couchdb -  the couchdb options
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_hpms_db - the hpms grid modeling result database
 * @param {Object} task.options.couchdb.db - defaults to grid_merge_couchdbquery_hpms_db, use this variable to override for some reason, say while testing.
 * @param {Function} cb -  this is an async function, so will call this fn with result.  Parameters will be error or null, and an Object with keys
[grid_id][year]['n','hh','nhh'] providing the scale factor for each.
 * @returns {null}
 * @throws {}
 */
function get_hpms_scales(task,cb){
    var view = "_design/calvad/_view/grid_year_day_hour" // incorrectly named!
    var _task = {year:task.year,
                 ts:task.ts,
                 options:{}}
    _task.options.couchdb = _.assign({},task.options.couchdb) // local copy
    if(_task.options.couchdb.db === undefined){
        _task.options.couchdb.db = _task.options.couchdb.grid_merge_couchdbquery_hpms_db
    }
    check_if_year_db(_task,function(e,yropts){
        var opts = _.assign({}
                       ,yropts.options.couchdb
                            ,{'view':view
                              ,'include_docs':false
                              ,'reduce':true
                              ,'group':true
                              ,'group_level':2
                             })
        getter(opts
               ,function(err,docs){
                   var aadt_fractions_names = ['n','hh','nhh']
                   var result = {}
                   if(docs !== undefined
                      && docs.rows !== undefined
                      && docs.rows.length>0){
                       // populate the result object
                       _.forEach(docs.rows,function(row){
                           var geom_id = row.key[0]
                           var year = row.key[1]
                           var days = row.value[3]/24 // last value is sum of hours.  24 hours in a day, so divide by 24 to get days
                           if(result[year] === undefined){
                               result[year] = {}
                           }
                           result[year][geom_id]={}

                           _.each(aadt_fractions_names,function(k,i){
                               var v = row.value[i]
                               if(v){ // null or zero v is an error

                                   // if v < days, then need to scale up
                                   // if v > days, then need to scale down
                                   // ideally, v === days (365 = 365) and scale is 1
                                   result[year][geom_id][k]=days/v
                               }else{
                                   console.log('warning v is undefined for ', row)
                                   result[year][geom_id][k]=0
                               }
                               return null
                           })
                           return null
                       })
                   }else{
                       if(docs !== undefined && docs.error !== undefined){
                           throw new Error(docs)
                       }
                   }

                   return cb(null,result)
               })
        return null
    })
    return null
}

/**
 * get_hpms_fractions  Gets the fractions for a given cell id and year.
 *
 * @param {Object} task
 * @param {Object} task.cell_id - the cell id.  combines with year to set start key
 * @param {Object} task.year - the year.  combines with cell id to set start key
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_hpms_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_hpms_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {Function} cb
 * @returns {}
 * @throws {}
 */
function get_hpms_fractions(task,cb){
    // go to couchdb, get fractions for year for this cell_id
    var _task = {year:task.year,
                 ts:task.ts,
                 options:{}}
    _task.options.couchdb = _.assign({},task.options.couchdb) // local copy
    if(_task.options.couchdb.db === undefined){
        _task.options.couchdb.db = _task.options.couchdb.grid_merge_couchdbquery_hpms_db
    }
    check_if_year_db(_task,function(e,yropts){
        if(e) throw new Error(e)
        var key = [task.cell_id,task.year].join('_')
        var opts = _.assign({}
                            ,yropts.options.couchdb
                            ,{startkey:key
                              ,'endkey':key+'-13' // there is no month 13; gets entire year
                              ,'view':'_all_docs' // alldocs, not any extraneous view
                              ,'include_docs':true})
        getter(opts
               ,function(err,docs){
                   var hours = 0
                   var sums = {} // accumulate summed value on the fly
                   var aadt_element = 'aadt_frac'
                   if(err) throw new Error(err)
                   task.hpms_fractions = {}
                   if(task.hpms_fractions_sums){
                       sums = task.hpms_fractions_sums
                   }
                   if(task.hpms_fractions_hours === undefined){
                       task.hpms_fractions_hours = 0
                   }
                   if(docs !== undefined
                      && docs.rows !== undefined
                      && docs.rows.length>0){
                       //console.log(docs.rows.length + ' rows from hpms_fractions query')
                       //console.log('doc keys',Object.keys(docs.rows[0].doc))
                       // temporary hack.  until 2012 is fixed, need to do this
                       if(docs.rows[0].doc.aadt_frac === undefined){
                           if( docs.rows[0].doc.aadt !== undefined){
                               aadt_element = 'aadt'
                           }else{
                               // oops
                               throw new Error('couchdb docs missing both aadt and aadt_frac')
                           }
                       }
                       _.each(docs.rows[0].doc[aadt_element]
                              ,function(v,k){
                                  if(sums[k] === undefined){
                                      sums[k]=+0
                                  }
                              })

                       _.each(docs.rows,function(row){
                           task.hpms_fractions[row.doc.ts]={}
                           hours++
                           _.each(row.doc[aadt_element],function(v,k){
                               if(!v) v = 0 // force null, undefined to be zero
                               task.hpms_fractions[row.doc.ts][k]=+v
                               sums[k]+=+v
                           })
                       })
                   }else{
                       //console.log('hpms_fractions nothing')
                       if(docs.error !== undefined){
                           throw new Error(docs)
                       }
                   }
                   task.hpms_fractions_sums=sums
                   task.hpms_fractions_hours = task.hpms_fractions_hours > hours ?
                       task.hpms_fractions_hours : hours
                   return cb(null,task)
               })
        return null
    })
    return null
}

/**
 * Get detector for all state for specified hour
 * @param {Object} task
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_detector_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_detector_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {string} task.ts - The exact hour to fetch
 * @param {Funtion} cb
 * @returns {}
 * @throws {}
 * private
 */
function get_detector_fractions_one_hour(task,cb){
    // go to couchdb, get fractions for an hour for all cells

    // all detectors, just one hour
    // hard code this view for now
    var view = "_design/by_hour/_view/hour_id_aadt_frac"
    var _task = {year:task.year,
                 ts:task.ts,
                 options:{}}
    _task.options.couchdb = _.assign({},task.options.couchdb) // local copy
    if(_task.options.couchdb.db === undefined){
        _task.options.couchdb.db = _task.options.couchdb.grid_merge_couchdbquery_detector_db
    }
    check_if_year_db(_task,function(e,yropts){
        var date = task.ts
        var opts = _.assign({}
                            ,yropts.options.couchdb
                            ,{'view':view
                              ,'include_docs':true
                              ,'startkey':[date]
                              ,'endkey':[date+'A'] // some random character
                             })
        if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_detector_db
        getter(opts
               ,function(err,docs){
                   var cells = 0
                   var aadt_element = 'aadt_frac'
                   var detector_fractions_cells = 0
                   task.detector_fractions = {}
                   task.detector_data = {}
                   // var id_regex = /$(\d*_\d*)/;
                   if(err){
                       console.log(err)
                       throw new Error(err)
                   }
                   if(docs !== undefined
                      && docs.rows !== undefined
                      && docs.rows.length>0){
                       _.forEach(docs.rows,function(row){
                           task.detector_fractions[row.key[1]]={}
                           cells++
                           if(Array.isArray(row.doc.data[0])){
                               task.detector_data[row.key[1]]=row.doc.data
                           }else{
                               task.detector_data[row.key[1]]=[row.doc.data]
                           }
                           _.each(row.doc.aadt_frac,function(v,k){
                               if(!v) v = 0 // force null, undefined to be zero
                               if(k==='not_hh'){
                                   task.detector_fractions[row.key[1]].nhh=+v
                               }else{
                                   task.detector_fractions[row.key[1]][k]=+v
                               }
                               return null
                           })
                           return null
                       })
                       task.detector_fractions_cells = cells
                   }
                   return cb(null,task)
               })
        return null
    })
}


/**
 * get detector fractions, one detector, per year
 * @param {} task
 * @param {Object} task.cell_id - the cell id to mark
 * @param {Object} task.year - the year
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.db - The database to hit.  If not defined, then will look for grid_merge_couchdbquery_detector_db
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_detector_db - The database to hit.  Will only be used if task.options.couchdb.db is not set
 * @param {} cb
 * @returns {}
 * @throws {}
 */
function get_detector_fractions(task,cb){
    // go to couchdb, get fractions for year for this cell_id
    // also, fix the stupid inconsistency:  not_hh == nhh
    var _task = {year:task.year,
                 ts:task.ts,
                 options:{}}
    _task.options.couchdb = _.assign({},task.options.couchdb) // local copy
    if(_task.options.couchdb.db === undefined){
        _task.options.couchdb.db = _task.options.couchdb.grid_merge_couchdbquery_detector_db
    }
    check_if_year_db(_task,function(e,yropts){

        var key = [task.cell_id,task.year].join('_')
        //console.log(key)
        var opts = _.assign({}
                       ,yropts.options.couchdb
                       ,{startkey:key
                        ,'endkey':key+'-13' // there is no month 13; gets entire year
                        ,'view':'_all_docs' // alldocs, not any extraneous view
                         ,'include_docs':true})
        getter(opts
               ,function(err,docs){
                   var hours = 0
                   var sums = {} // accumulate summed value on the fly
                   if(err) throw new Error(err)
                   task.detector_fractions = {}
                   task.detector_data = {}
                   if(task.detector_fractions_sums){
                       sums = task.detector_fractions_sums
                   }
                   if(task.detector_fractions_hours === undefined){
                       task.detector_fractions_hours = 0
                   }
                   if(docs !== undefined
                      && docs.rows !== undefined
                      && docs.rows.length>0){
                       _.each(docs.rows[0].doc.aadt_frac
                              ,function(v,k){
                                  if(k==='not_hh'){
                                      k='nhh'
                                  }
                                  if(sums[k] === undefined){
                                      sums[k]=+0
                                  }
                                  return null
                              })

                       _.each(docs.rows,function(row){
                           // if data is an array, use zero.  if data is an array of arrays, use [0][0]
                           // console.log(row)
                           var ts = row.doc.data[0]
                           // console.log(ts)
                           if(Array.isArray(ts)){
                               ts=row.doc.data[0][0]
                           }
                           // console.log(ts)
                           task.detector_fractions[ts]={}
                           task.detector_data[ts]=row.doc.data
                           hours++
                           _.each(row.doc.aadt_frac,function(v,k){
                               if(!v) v = 0 // force null, undefined to be zero
                               if(k==='not_hh'){
                                   sums.nhh+=+v
                                   task.detector_fractions[ts].nhh=+v
                               }else{
                                   sums[k]+=+v
                                   task.detector_fractions[ts][k]=+v
                               }
                               return null
                           })
                           return null
                       })
                   }else{
                       //console.log('detector_fractions nothing')
                       if(docs.error !== undefined){
                           throw new Error(JSON.stringify(docs))
                       }
                   }
                   task.detector_fractions_sums=sums
                   task.detector_fractions_hours = task.detector_fractions_hours > hours ?
                       task.detector_fractions_hours : hours
                   return cb(null,task)
               })
        return null
    })
    return null
}

/**
 * put_results_doc
 *
 * put a document into couchdb
 *
 * @param {Object} task
 * @param {Object} task.doc - the document to put into couchdb.  if
 * doc.id is not defined, couchdb will automatically generate one.  if
 * the id is defined, and if the doc already exists, then all of the
 * values set in the document will overwrite the existing document.
 * See the documentation for jmarca/couchdb_put_doc for details on how
 * overwrite=true works.
 * @param {Object} task.year - the year
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_put_db - The database to use for putting data.  Required.  Note that data will not be put, just checked.
 * @param {Function} callback - if the call to couchdb does not error,
 * then the callback will get null, and the task object passed to this
 * function as its second argument
 * @returns {}
 * @throws {Error} - will throw if
 * task.options.couchdb.grid_merge_couchdbquery_put_db is not defined.
 */
function put_results_doc(task,callback){
    var options, put
    // reformat for the library call
    if(task.options.couchdb.grid_merge_couchdbquery_put_db===undefined){
        throw new Error('must define options.couchdb.grid_merge_couchdbquery_put_db in the task object')
    }
    options = {'cuser':task.options.couchdb.auth.username
                  ,'cpass':task.options.couchdb.auth.password
                  ,'chost':task.options.couchdb.url
                  ,'cport':task.options.couchdb.port
                  ,'cdb'  :task.options.couchdb.grid_merge_couchdbquery_put_db
                  }
    // make the putter
    put = putter(options)
    put.overwrite(true)
    // does the doc have an id? if not, it will be autogenerated
    put(task.doc,function(err,responsebody){
        return callback(err,task)
    })

}

/**
 * check_results_doc
 *
 * check the results doc
 *
 * @param {Object} task
 * @param {Object} task.doc - the document of interest
 * @param {Object} task.doc.id - the id of the document that will be used for lookup/checking
 * @param {Object} task.year - the year
 * @param {Object} task.options - what was loaded from config.json?
 * @param {Object} task.options.couchdb - couchdb connection details
 * @param {Object} task.options.couchdb.grid_merge_couchdbquery_put_db - The database to use for putting data.  Required.  Note that data will not be put, just checked.
 * @param {Function} cb - if the call to couchdb does not error, then
 * the callback will get null, and true or false.  True if the
 * document exists already in couchdb, false if it does not yet exist.
 * @returns {} - null
 * @throws {Error} - will throw if
 * task.options.couchdb.grid_merge_couchdbquery_put_db is not defined.
 * Will also throw from the async call to couchdb if the couchdb call
 * generates an error
 */
function check_results_doc(task,cb){
    // reformat for the library call
    var options
    if(task.options.couchdb.grid_merge_couchdbquery_put_db===undefined){
        throw new Error('must define options.couchdb.grid_merge_couchdbquery_put_db in the task object')
    }
    options = _.assign({},task.options.couchdb
                           ,{doc:task.doc.id})
    options.db = task.options.couchdb.grid_merge_couchdbquery_put_db

    // make the state checker
    checker.check_exists(options
                        ,function(err,revision){
                             var result = false
                             if(err) throw new Error(err)
                             if(revision !== undefined){
                                 result=true
                             }
                             return cb(null,result)
                         })
    return null
}


exports.filter_out_done=filter_out_done
exports.mark_done=mark_done
exports.mark_in_process=in_process
exports.get_hpms_fractions=get_hpms_fractions
exports.get_hpms_fractions_one_hour=get_hpms_fractions_one_hour
exports.get_detector_fractions=get_detector_fractions
exports.get_detector_fractions_one_hour=get_detector_fractions_one_hour
exports.check_results_doc=check_results_doc
exports.put_results_doc=put_results_doc
exports.get_hpms_scales=get_hpms_scales
