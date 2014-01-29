/*global require exports */

// map reduce.  this is the reduce part.  suppose there is a map of
// tasks, well there is.  now I am passed a single task, and ignoring
// all the others, I am going to process the task and report its
// results.

// except, apparently I am keeping the tally in the task.  Hmm.  That
// changes how the iteration happens, I guess

var cdb_interactions = require('../lib/couchdb_interactions')
var filter_grids = cdb_interactions.filter_out_done
var mark_done = cdb_interactions.mark_done
var in_process = cdb_interactions.mark_in_process
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

var async=require('async')
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
for (var i=0,j=header.length; i<j; i++){
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
    });
    _.each(item.detector_data,function(record,ts){
        // could also insert speed here into to the sum by
        // multiplying by n to weight it, as I do elsewhere
        var detector_miles = record[unmapper.miles]
        if(memo[ts]===undefined){
            memo[ts]={}
        }
        if(memo[ts]['detector_based']===undefined){
            memo[ts]['detector_based']={'n':record[unmapper.n]
                                       ,'n_mt':record[unmapper.n]*detector_miles
                                       ,'hh_mt':record[unmapper.hh]*detector_miles
                                       ,'nhh_mt':record[unmapper.not_hh]*detector_miles
                                       ,'lane_miles':record[unmapper.lane_miles]
                                       }
        }else{
            memo[ts]['detector_based'].n      += record[unmapper.n]
            memo[ts]['detector_based'].n_mt   += record[unmapper.n]*detector_miles
            memo[ts]['detector_based'].hh_mt  += record[unmapper.hh]*detector_miles
            memo[ts]['detector_based'].nhh_mt += record[unmapper.not_hh]*detector_miles
            memo[ts]['detector_based'].lane_miles += record[unmapper.lane_miles]
        }
    });
    return callback(null,memo)
}

/**
 * handle_detector_data
 *
 */
function handle_detector_data(memo,item,cb){
    // add in the detector-based data to the task, mixing in with aadt_store
}

// fixme need a test on this for sure //

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

    task.accum = {} // reset the accumulator here
    var scale = task.scale

    _.each(task.aadt_store,function(aadt_record,road_class){
        // write out lane miles and initialize structure
        _.each(task.fractions,function(record,timestamp){
            if(task.accum[timestamp]===undefined){
                task.accum[timestamp]={}
            }
            if(task.accum[timestamp][road_class]===undefined){
                task.accum[timestamp][road_class]={}
            }
            task.accum[timestamp][road_class]['sum_lane_miles']
             = aadt_record['sum_lane_miles']
        });

        _.each(nhh_vars,function(k2){
            // multiply by nhh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.nhh * scale.nhh * aadt_record[k2]
            });
        });
        _.each(n_vars,function(k2){
            // multiply by n
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.n * scale.n * aadt_record[k2]
            });
        });
        _.each(hh_vars,function(k2){
            // multiply by hh
            _.each(task.fractions,function(record,timestamp){
                task.accum[timestamp][road_class][k2] = record.hh * scale.hh * aadt_record[k2]
            });
        });
    });
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
    task.fractions = task.detector_fractions
    var scale = {'n': 1
                ,'hh': 1
                ,'nhh': 1
                }
    // do not scale detector fractions, as they are correct by
    // construction

    if(task.detector_fractions === undefined || _.isEmpty(task.fractions)){
        // cover all the bases, belt and suspenders and all that
        if(task.hpms_fractions === undefined || _.isEmpty(task.hpms_fractions)){
            return cb(null,task) // or error?
        }
        task.fractions = task.hpms_fractions
        var days = task.hpms_fractions_hours / 24

        _.each(scale,function(v,k){
            // scale such that we sum to a correct number of days sum
            // of fractionals should add up to total number of days in
            // set by year this makes sense, for shorter periods not
            // so much as traffic in one part of the year might be
            // heavier than another, thus leading to the total aadt
            // being skewed throughout the year

            // anyway, best available type of solution.  Better data would be nice
            scale[k] = days / task.hpms_fractions_sums[k]
        });

    }
    task.scale=scale
    return cb(null,task)
}


/**
 * post_process_sql_queries
 *
 * pass in a task after it has hit postgres,
 *
 * returns a modified task, with duplicate roads from detectors
 * removed from the hpms sums, so that you don't double count
 *
 */
function post_process_sql_queries(task,cb){
    // this function merges the sql output from hpms and detectors

    // first, isolate the highways that are covered by detectors,
    // remove from hpms subtotals
    var detector_routes = _.unique(_.pluck(task.detector_route_numbers, 'route_number')) || []
    var hpms_routes = _.unique(_.pluck(task.accum, 'route_number')) || []
    var keep_hpms = _.difference(detector_routes,hpms_routes)
    var drop_hpms = _.difference(detector_routes,keep_hpms)

    // so only the "keep_hpms" highways get kept when summing up data

    var f_system_list = _.unique(_.pluck(task.accum,'f_system'))
    var class_map={}
    var store = {}
    // filter out accum values with objectionable
    _.each(task.accum,function(row){
        var f = row.f_system

        if(_.indexOf(drop_hpms,row.route_number) === -1){
            // not on the skip list, so keep going
            // iterate over the types of sums for this functional class
            if(store[f] === undefined){
                store[f]={'sum_vmt':0
                         ,'sum_lane_miles':0
                         ,'sum_single_unit_mt':0
                         ,'sum_combination_mt':0
                         }
            }
            _.each(store[f],function(v,k){
                if(row[k]){
                    store[f][k]+=row[k]
                }
                return null
            })
        }else{
            // dropping this hpms data, but track the functional classification?
            if(row.route_number){
                if(class_map[row.route_number] === undefined){
                    class_map[row.route_number]=[f]
                }else{
                    class_map[row.route_number].push(f)
                }
            }
        }
    });

    // now for each functional class, I have AADT values.  Multiply
    // those by the grid's hourly fraction, and you get the hourly
    // AADT, etc.

    // I think that is a different function's job

    task.aadt_store = store
    task.class_map = class_map
    return cb(null,task)
}

exports.post_process_sql_queries=post_process_sql_queries
exports.post_process_couch_query=post_process_couch_query
exports.apply_fractions=apply_fractions
exports.reduce=reduce
