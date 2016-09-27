/*global require exports */
"use strict"

// map reduce.  this is the reduce part.  suppose there is a map of
// tasks, well there is.  now I am passed a single task, and ignoring
// all the others, I am going to process the task and report its
// results.

// except, apparently I am keeping the tally in the task.  Hmm.  That
// changes how the iteration happens, I guess

var queue = require('d3-queue').queue
var _ = require('lodash')
var get_hpms_scales = require('./couchdb_interactions').get_hpms_scales
// hard code some stuff, so I don't accidentally multiple lane miles
// by the hourly fraction which of course is nonsensical
var n_vars = ['sum_vmt']
var nhh_vars=['sum_single_unit_mt']
var hh_vars =['sum_combination_mt']

var view_fractions = ['n','hh','nhh','days']

var header=["ts"
           ,"freeway"
           ,"n"
           ,"hh"
           ,"not_hh"
           ,"o"
           ,"avg_veh_spd"
           ,"avg_hh_weight"
           ,"avg_hh_axles"
           ,"avg_hh_spd"
           ,"avg_nh_weight"
           ,"avg_nh_axles"
           ,"avg_nh_spd"
           ,"miles"
           ,"lane_miles"
           ,"detector_count"
           ,"detectors"]
var unmapper = {}
var i, j
for (i=0,j=header.length; i<j; i++){
    unmapper[header[i]]=i
}

/**
 * estimate_scale
 * async because if isn't defined, have to go get it from couchdb
 * @param {string} grid
 * @param {integer} year
 * @param {Function} cb
 */
var get_scales = (function (){
    var hpms_scale
    return function(task,cb){
        if(hpms_scale === undefined){
            // go fetch
            hpms_scale={}
            get_hpms_scales(task,function(e,r){
                _.assign(hpms_scale,r)
                return cb(e,hpms_scale[task.year])
            })
            return null
        }else{
            return cb(null,hpms_scale[task.year])
        }
    }
})()


/**
 * aggregate
 *
 * call after calling apply_fractions to aggregate together task data
 *
 * returns a modified task, with duplicate roads from detectors
 * removed from the hpms sums, so that you don't double count
 *
 */
function reduce(memo,item,callback){
    // combine item.accum into memo

    // not doing speed or speed limit or whatever at the moment
    // hpms only has design speed and speed limit
    _.each(item.accum,function(roads,ts){
        if(memo[ts]===undefined){
            memo[ts]=_.clone(roads,true)
        }else{
            _.each(roads,function(record,road_class){
                if(memo[ts][road_class]===undefined){
                    memo[ts][road_class]=_.clone(record,true)
                }else{
                    _.each(record,function(v,k){
                        memo[ts][road_class][k] += v
                    })
                }
            })
        }
    })
    _.each(item.detector_data,function(record,ts){
        // could also insert speed here into to the sum by
        // multiplying by n to weight it, as I do elsewhere
        var detector_miles = record[unmapper.miles]
        if(memo[ts]===undefined){
            memo[ts]={}
        }
        if(memo[ts].detector_based===undefined){
            memo[ts].detector_based={//'n':record[unmapper.n]
                                       'n_mt':record[unmapper.n]
                                       ,'hh_mt':record[unmapper.hh]
                                       ,'nhh_mt':record[unmapper.not_hh]
                                       ,'lane_miles':record[unmapper.lane_miles]
                                       }
        }else{
            //memo[ts].detector_based.n      += record[unmapper.n]
            memo[ts].detector_based.n_mt   += record[unmapper.n]
            memo[ts].detector_based.hh_mt  += record[unmapper.hh]
            memo[ts].detector_based.nhh_mt += record[unmapper.not_hh]
            memo[ts].detector_based.lane_miles += record[unmapper.lane_miles]
        }
    })
    return callback(null,memo)
}


/**
 * apply_fractions
 *
 * multiply aadt by the hour's fractional part
 *
 */
function apply_fractions(task,cb){
    // now multiply the array, return the result
    // notes:
    //
    // inside of each loop, the record is the record for that hour,
    // the task is where things get accumulated, and the scale
    // variable is the thing that makes sure that I don't over weight
    // anything

    var scale = task.scale
    task.accum = {} // reset the accumulator here

    _.each(task.aadt_store,function(aadt_record,road_class){
        // write out lane miles and initialize structure
        _.each(task.fractions,function(record,timestamp){
            if(task.accum[timestamp]===undefined){
                task.accum[timestamp]={}
            }
            if(task.accum[timestamp][road_class]===undefined){
                task.accum[timestamp][road_class]={}
            }
            task.accum[timestamp][road_class].sum_lane_miles
             = aadt_record.sum_lane_miles
        })
        _.each(nhh_vars,function(k2){
            // multiply by nhh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.nhh * scale.nhh * aadt_record[k2]
            })
        })
        _.each(n_vars,function(k2){
            // multiply by n
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.n * scale.n * aadt_record[k2]
            })
        })
        _.each(hh_vars,function(k2){
            // multiply by hh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.hh * scale.hh * aadt_record[k2]
            })
        })
    })
    // take out the trash if needed
    // task.fractions = null
    return cb(null,task)
}

/**
 * post_process_couch_query
 *
 * create a single fractions thing, merging detector_fractions and
 * hpms_fractions, and also for hpms_fractions, create a scaling
 * factor so that all of the hourly fractions will sum up to the
 * number of days in the year.
 *
 * @param {} task
 * @param {} cb
 * @returns {}
 * @throws {}
 */
function post_process_couch_query(task,cb){
    // this function generates a scaling factor for hpms-only grids,
    // so that they sum up to the total number of days in the year.

    var days
    var scale = {'n': 1
                ,'hh': 1
                ,'nhh': 1
                }
    task.fractions={}
    // do not scale detector fractions, as they are correct by
    // construction

    if(task.detector_fractions !== undefined && ! _.isEmpty(task.detector_fractions)){
        task.fractions = task.detector_fractions
        task.scale=scale
        return cb(null,task)

    }else{
        // cover all the bases, belt and suspenders and all that
        if(task.hpms_fractions === undefined || _.isEmpty(task.hpms_fractions)){
            console.log('no fractions loaded?? for task.cell_id=',task.cell_id)
            // throw new Error('no fractions?')
            task.scale=scale
            return cb(null,task) // or error?
        }else{

            task.fractions = task.hpms_fractions
            get_scales(task,function(e,_scales){
                task.scale = {}
                _.each(scale,function(v,k){
                    if(_scales[task.cell_id][k] === undefined){
                        throw new Error ('scale not defined for key ', k)
                    }
                    task.scale[k] = _scales[task.cell_id][k]
                })
                return cb(null,task)
            })
        }

    }
}

/**
 * post_process_couch_query_one_hour
 *
 * create a single fractions thing, merging detector_fractions and
 * hpms_fractions.  Scale factors are set to one, because not enough
 * information here to compute proper scaling factor as in the year
 * case
 *
 * @param {} task
 * @param {} cb
 * @returns {}
 * @throws {}
 */
function post_process_couch_query_one_hour(task,cb){
    // this function generates a scaling factor for hpms-only grids,
    // so that they sum up to the total number of days in the year.
    task.fractions = {}
    task.scale={}  // one entry per grid cell


    // what I have to do here is merge the cellid keys from detectors and hpms grids.  If there is any overlap, that is cause for concern so throw.

    // otherwise, a cell has fractions from the HPMS model runs, or
    // from detector-based data

    // every cell with a fraction should be the output for this routine.


    var days
    var detector_scale = {'n': 1
                          ,'hh': 1
                          ,'nhh': 1
                         }
    // do not scale detector fractions, as they are correct by
    // construction


    // first, copy in detector-based fractions, and add a flag saying
    // that it is detector based perhaps?

    // also, for each detector id, scale is one
    _.forEach(task.detector_fractions,function(entry,cellid){
        task.scale[cellid]=detector_scale
        task.fractions[cellid] = entry
        return null
    })

    // okay, with that tricky task out of the way, now add in all of the hpms-model output fractions
    _.forEach(task.hpms_fractions,function(value,cellid){
        // there could be an existing cell in output.  If so, croak I guess
        if(task.fractions[cellid] !== undefined) {
            var errmsg = 'need to check why '+cellid+' is defined in both hpms and detectors for fractions for date '+task.ts + '.  Sticking with detector version.'
            console.log(errmsg)
            //throw new Error()
        }else{
            // okay, that baggage handled, on with the show
            task.fractions[cellid] = value
        }
        return null
    })
    // now get the scale factors for this grid cell
    // this needs to be per grid cell
    get_scales(task,function(e,_scale){
        // be careful to make sure that detector-based cells are
        // always 1,1,1 even if, as is sometimes the case, there is a
        // "modeled" version of fractions for a grid as well as
        // detector-based data
        task.scale = _.assign({}, _scale, task.scale)
        return cb(null,task)
    })
    return null
}




exports.post_process_couch_query=post_process_couch_query
exports.post_process_couch_query_one_hour=post_process_couch_query_one_hour
exports.reduce=reduce
