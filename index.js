var fs = require('fs'),
    Knex = require('knex'),
    PATH = require('path'),
    Logger = require('logger'),
    Promise = require('bluebird');

var baseDir = PATH.dirname(__dirname),
    log = new Logger('Migration', 'trace');

module.exports = Flight;

function Flight(config) {
    log.trace('new Flight()');
    
    config = config || { };
    if (!config.db) { throw new Error('No database configuration supplied'); }
    this.knex = Knex.initialize({
        client: config.db.client,
        connection: {
            host: config.db.host,
            user: config.db.user,
            password: config.db.pass,
            database: config.db.name,
            charset: 'utf8'
        }
    });
    this.dry = config.dry || false;
    this.path = config.path || PATH.join(baseDir, 'migrations');
    this.mask = config.math || /^(\d+)-.*\.js$/;
    this.dist = typeof config.distance === 'number' ? config.distance : 1;
    this.pos = typeof config.position === 'number' ? config.position : 0;
    this.items = [ ];
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
    while (i < x && curPos > self.items[i].idx) {
        console.log(curPos, '<=', self.items[i].idx, '?');
        i++;
    }
    
    // apply migrations
    while (i < x && count) {
        (function (item, newPos) {
            log.silly('Migrating up: ' + item.name);
            if (self.dry) {
                promise = promise.then(item.dryUp());
            } else {
                promise = promise.then(function () {
                    var res = item.up();
                    return res.tap(function () {
                        self.setPos(newPos);
                    }).catch(function (err) {
                        if (item.recover) {
                            return item.recover().throw(err);
                        } else {
                            throw err;
                        }
                    });
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
    while (i < x && curPos <= self.items[i].idx) {
        console.log(curPos, '<=', self.items[i].idx, '?');
        i++;
    }
    i--;
    
    // apply migrations
    while (i >= 0 && count) {
        (function (item, newIdx) {
            log.silly('Migrating down: ' + item.name);
            if (self.dry) {
                promise = promise.then(item.dryDown());
            } else {
                promise = promise.then(function () {
                    var res = item.down();
                    return res.tap(function () {
                        self.setPos(newIdx);
                    });
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
    this.knex.client.pool.destroy();
};

function Migration(opts, deps) {
    this.fullPath = opts.path;
    this.name = PATH.basename(opts.path, '.js');
    this.idx = opts.idx;
    var mod = require(opts.path)(deps);
    this.up = mod.up;
    this.down = mod.down;
    this.recover = mod.recover;
}
