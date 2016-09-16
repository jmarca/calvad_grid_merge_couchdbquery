/*global require exports */
"use strict"

// map reduce.  this is the reduce part.  suppose there is a map of
// tasks, well there is.  now I am passed a single task, and ignoring
// all the others, I am going to process the task and report its
// results.

// except, apparently I am keeping the tally in the task.  Hmm.  That
// changes how the iteration happens, I guess


var _ = require('lodash')

// hard code some stuff, so I don't accidentally multiple lane miles
// by the hourly fraction which of course is nonsensical
var n_vars = ['sum_vmt']
var nhh_vars=['sum_single_unit_mt']
var hh_vars =['sum_combination_mt']

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
            memo[ts].detector_based={'n':record[unmapper.n]
                                       ,'n_mt':record[unmapper.n]*detector_miles
                                       ,'hh_mt':record[unmapper.hh]*detector_miles
                                       ,'nhh_mt':record[unmapper.not_hh]*detector_miles
                                       ,'lane_miles':record[unmapper.lane_miles]
                                       }
        }else{
            memo[ts].detector_based.n      += record[unmapper.n]
            memo[ts].detector_based.n_mt   += record[unmapper.n]*detector_miles
            memo[ts].detector_based.hh_mt  += record[unmapper.hh]*detector_miles
            memo[ts].detector_based.nhh_mt += record[unmapper.not_hh]*detector_miles
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
        console.log('processing '+road_class)
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
 * make a single fractions thing.  just modularizing code here
 */

function post_process_couch_query(task,cb){
    // this function multiplies each hour in the fractions array in
    // the task by each roadway type's AADT, VMT
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
    }else{
        // cover all the bases, belt and suspenders and all that
        if(task.hpms_fractions === undefined || _.isEmpty(task.hpms_fractions)){
            console.log('no fractions loaded?? for task.cell_id=',task.cell_id)
            // throw new Error('no fractions?')
            // return cb(null,task) // or error?
        }else{
            task.fractions = task.hpms_fractions
            days = task.hpms_fractions_hours / 24
            console.log('days is',days)
            console.log('task.hpms_fractions_sums',task.hpms_fractions_sums)
            _.each(scale,function(v,k){
                // scale such that we sum to a correct number of days sum
                // of fractionals should add up to total number of days in
                // set by year this makes sense, for shorter periods not
                // so much as traffic in one part of the year might be
                // heavier than another, thus leading to the total aadt
                // being skewed throughout the year

                // anyway, best available type of solution.  Better data would be nice
                scale[k] = days / task.hpms_fractions_sums[k]
            })
        }

    }
    task.scale=scale
    return cb(null,task)
}


exports.post_process_couch_query=post_process_couch_query
exports.reduce=reduce
