var cdbi = require('./lib/couchdb_interactions.js')
var mmap = require('./lib/make_map.js')
var reduce = require('./lib/reduce.js')

exports.filter_out_done= cdbi.filter_out_done
exports.mark_done= cdbi.mark_done
exports.mark_in_process= cdbi.in_process
exports.get_hpms_fractions= cdbi.get_hpms_fractions
exports.get_detector_fractions= cdbi.get_detector_fractions
exports.get_hpms_fractions_one_hour= cdbi.get_hpms_fractions_one_hour
exports.get_detector_fractions_one_hour= cdbi.get_detector_fractions_one_hour_V2
exports.put_results_doc= cdbi.put_results_doc
exports.check_results_doc= cdbi.check_results_doc


exports.all_tasks=mmap.all_tasks

exports.post_process_couch_query=reduce.post_process_couch_query
exports.post_process_couch_query_one_hour=reduce.post_process_couch_query_one_hour
