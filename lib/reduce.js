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

function merge_hpms_detectors_data(task,cb){
    // this function merges the sql output from hpms and detectors

    // first, isolate the highways that are covered by detectors,
    // remove from hpms subtotals
    var detector_routes = task.detector_route_numbers
    var hpms_routes = _.unique(_.pluck(task.accum, 'route_number'))
    var keep_hpms = _.difference(detector_routes,hpms_routes)

    handle_detectors_highways(task)
    hpms_highways(detectors_highways,task)

}