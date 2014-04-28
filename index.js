var fs = require('fs'),
    PATH = require('path'),
    Logger = require('logger'),
    Promise = require('bluebird');

var baseDir = PATH.dirname(__dirname),
    log = new Logger('Migration', 'silly');

module.exports = Flight;

function Flight(config) {
    log.trace('new Flight()');
    
    config = config || { };
    if (!config.knex) { throw new Error('Please supply an instance of knex'); }
    this.knex = config.knex;
    this.dry = config.dry || false;
    this.path = config.path || PATH.join(baseDir, 'migrations');
    log.silly('Using path: ' + this.path);
    this.mask = config.math || /^(\d+)-.*\.js$/;
    this.dist = typeof config.distance === 'number' ? config.distance : 1;
    this.items = [ ];
    
    var p = PATH.join(this.path, '.flight'), q;
    if (typeof config.position === 'number') {
        log.info('Configured at position: ' + config.position);
        this.pos = config.position;
    } else if (fs.existsSync(p)) {
        q = parseInt(fs.readFileSync(p), 10);
        if (typeof q === 'number' && Number.isFinite(q)) {
            log.info('Loaded position from file: ' + q);
            this.pos = q;
        } else {
            // mind your p's and q's :V
            log.warn(p + ' contained an invalid value: ' + q);
        }
    }
    if (this.pos === void 0) {
        log.info('Defaulting to position: ' + 0);
        this.pos = 0;
    }
};
Flight.prototype.setPos = function (pos) {
    log.trace('Position updated to: ' + pos);
    
    this.pos = pos;
};
Flight.prototype.upTo =
Flight.prototype.up = function (_target) {
    log.trace('Flight.up()');

    if (this.items.length === 0) {
        return Promise.reject('Load some migrations first!');
    }

    var self = this,
        i = 0,
        x = self.items.length,
        target = _target || self.items[x-1].idx,
        curPos = self.pos,
        count = self.dist,
        promise = Promise.resolve();
    
    // seek to current position
    while (i < x && curPos >= self.items[i].idx) { i++; }
    
    // apply migrations
    while (i < x && count) {
        (function (item, newPos) {
            log.silly('Migrating up: ' + item.name);
            if (self.dry) {
                promise = promise.then(item.dryUp());
            } else {
                promise = promise.then(function () {
                    var res = item.up();
                    if (Promise.is(res)) {
                        res = res.tap(function (newPos) {
                            self.setPos(newPos);
                        }).catch(function (err) {
                            if (typeof item.recover === 'function') {
                                log.warn('Attempting to recover from error:', err);
                                return item.recover();
                            }
                            throw err;
                        });
                    } else {
                        self.setPos(newPos);
                    }
                    return res;
                });
            }
        })(self.items[i], self.items[i].idx);
        i++; count--;
    }
    
    return promise;
};

Flight.prototype.downTo =
Flight.prototype.down = function (_target) {
    log.trace('Flight.down()');
    
    if (this.items.length === 0) {
        return Promise.reject('Load some migrations first!');
    }

    var self = this,
        i = 0,
        x = self.items.length,
        target = _target || self.items[x-1].idx,
        curPos = self.pos,
        count = self.dist,
        promise = Promise.resolve();
    
    // seek to current position
    while (i < x && curPos <= self.items[i].idx) { i++; }
    i--;
    
    // apply migrations
    while (i >= 0 && count) {
        (function (item, newPos) {
            log.silly('Migrating down: ' + item.name);
            if (self.dry) {
                promise = promise.then(item.dryDown());
            } else {
                promise = promise.then(function () {
                    var res = item.down();
                    
                    if (Promise.is(res)) {
                        res = res.tap(function () {
                            self.setPos(newPos);
                        });
                    } else {
                        self.setPos(newPos);
                    }
                    
                    return res;
                });
            }
        })(self.items[i], i > 0 ? self.items[i-1].idx : 0);
        
        i--; count--;
    }
    
    return promise;
};
Flight.prototype.loadFiles = function () {
    log.trace('Flight.loadFiles()');

    var self = this,
        path = self.path,
        mask = self.mask;
    
    return Promise.promisify(fs.readdir)(path).filter(function (fileName) {
        return mask.test(fileName);
    }).map(function (fileName) {
        var fullPath = PATH.join(path, fileName);
        return Promise.promisify(fs.stat)(fullPath).tap(function (stat) {
            stat.fullPath = fullPath;
            stat.fileName = fileName;
        });
    }).filter(function (stat) {
        return stat.isFile();
    }).map(function (stat) {
        return new Migration({
            path: stat.fullPath,
            idx: stat.fileName.match(mask)[1]
        }, {
            log: log,
            knex: self.knex,
            Promise: Promise
        });
    }).then(function (migrations) {
        self.items.push.apply(self.items, migrations);
        return self.items.sort(function (a, b) { return a.idx - b.idx; });
    });
};

Flight.prototype.end = function () {
    log.info('Ending position: ' + this.pos);
    fs.writeFile(PATH.join(this.path, '.flight'), this.pos);
    this.knex.client.pool.destroy();
};

function Migration(opts, deps) {
    log.trace('new Migration()', opts);
    this.fullPath = opts.path;
    this.name = PATH.basename(opts.path, '.js');
    this.idx = opts.idx;
    var mod = require(opts.path)(deps);
    this.up = mod.up;
    this.down = mod.down;
    this.recover = mod.recover;
}
