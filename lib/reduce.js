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


function reduce_get_fractions(task,cb){
    // what?
    async.applyEach([get_hpms_fractions, get_detector_fractions]
                   ,task
                   ,function(err,results){
                        // I've no idea what might be in results.  nor do I care
                        if(err) throw new Error(err)
                        // I hope task is sorted, however.

                        // either HPMS happened, or Detectors
                        // happened, but not both, as they are
                        // mutually exclusive

                        cb(null,task)
                    })


}


function reduce_get_sql(task,cb){
    // so here, if it is detectors grid or if it is HPMS grid, the sql
    // query will be different.  If it is not a detectors grid, it is
    // simpler.  If it is, I need to get both the hpms data and the
    // detectors data, and then fiddle with merging the two sets.

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
    var detector_routes = task.detector_route_numbers || []
    var hpms_routes = _.unique(_.pluck(task.accum, 'route_number')) || []
    var keep_hpms = _.difference(detector_routes,hpms_routes)
    var drop_hpms = _.difference(detector_routes,keep_hpms)

    // so only the "keep_hpms" highways get kept when summing up data

    var f_system_list = _.unique(_.pluck(task.accum,'f_system'))
    var class_map={}
    var store = {}
    _.each(f_system_list,function(f){
        store[f]={'sum_aadt':0
                 ,'sum_vmt':0
                 ,'sum_lane_miles':0}
    });
    // filter out accum values with objectionable
    _.each(task.accum,function(row){
        var f = row.f_system
        if(_.indexOf(drop_hpms,row.route_number) !== -1){
            // not on the skip list, so keep going
            // iterate over the types of sums for this functional class
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
