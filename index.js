var Padlock = require('padlock').Padlock;

function RiakDownIncrement(parent, rdiOpts) {
    rdiOpts = rdiOpts || {};
    rdiOpts.sep = rdiOpts.sep || '!';
    var lock = new Padlock();

    function DB() {};
    DB.prototype = parent;
    var db = new DB();
    db.parent = parent;

    function getRoot(_db) {
        if (_db.parent) {
            return getRoot(_db.parent);
        }
        return _db;
    }
    var riak = getRoot(db).db;

    db.increment = function (key, value, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.parent.put(key, value, {
            bucket: opts.bucket,
            type: 'counter'
        }, callback);
    }

    db.getCount = function (key, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        db.parent.get(key, {
            bucket: opts.bucket,
            type: 'counter',
        }, function (err, reply) {
            callback(null, reply || 0);
        });
    };


    return db;
}

module.exports = RiakDownIncrement;
