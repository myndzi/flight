var fs = require('fs'),
    Knex = require('knex'),
    PATH = require('path'),
    Logger = require('logger'),
    Promise = require('bluebird');

Promise.promisifyAll(fs, { suffix: '$' });

var parentRoot = require('parent-root');

var baseDir = parentRoot(),
    log = new Logger('Migration');

module.exports = Flight;

function Flight(config) {
    log.trace('new Flight()');

    var self = this;
    config = config || { };
    
    if (!config.knex) {
        log.trace('Using in-memory sqlite3 database');
        this.knex = Knex({
            client: 'sqlite3',
            connection: { filename: ':memory:' }
        });
    } else if (typeof config.knex === 'function') {
        log.trace('Using provided instance of knex');
        this.knex = config.knex;
    } else {
        log.trace('Using provided database configuration');
        this.knex = Knex(config.knex);
    }
    
    this.dry = config.dry || false;
    this.path = config.path || PATH.join(baseDir, 'migrations');
    log.silly('Using path: ' + this.path);
    this.mask = config.math || /^(\d+)-.*\.js$/;
    this.dist = typeof config.distance === 'number' ? config.distance : 1;

    this._init = null;
    
    this.items = [ ];
    this.idx = -1;
    this.pos = config.hasOwnProperty('version') ? config.version : -1;
}
Flight.prototype.init = function () {
    log.trace('Flight.init()');
    
    var self = this;
    
    if (self._init) { return self._init; }
    
    return self._init = Promise.all([
        self.loadFiles(self.path),
        self.loadPos()
    ]).spread(function (items, pos) {
        self.items = items;
        self.pos = pos;
        self.idx = self.getIdx();
        self.initialized = true;
    });
};
Flight.prototype.loadPos = Promise.method(function () {
    log.trace('Flight.loadPos()');
    
    var self = this,
        knex = self.knex,
        pos = parseInt(self.pos, 10);
    
    if (!isNaN(pos) && pos >= 0) {
        log.info('Loaded position from configuration: ' + pos);
        return pos;
    }
    
    var versionFile = PATH.join(self.path, '.flight');

    log.trace('Checking for version in database');
    return knex.schema.hasTable('__flight')
    .then(function (exists) {
        if (exists) {
            return knex.select('version').from('__flight');
        }
        log.info('Creating system table...');
        return knex.schema.createTable('__flight', function () {
            this.string('version');
        })
        .then(function () {
            return knex.insert({ version: '0' }).into('__flight');
        }).return([{ version: '0' }]);
    })
    .then(function (rows) {
        if (!rows.length) { throw 'no results'; }
        var pos = parseInt(rows[0].version, 10);
        if (isNaN(pos)) { throw 'invalid'; }
        log.info('Loaded position from database: ' + pos);
        return pos;
    });
});

// find the last migration that has been run
Flight.prototype.getIdx = function () {
    log.trace('Flight.getIdx()');
    
    var self = this,
        items = self.items;
    
    if (items.length === 0) {
        return -1;
    }
    
    var low = 0, high = items.length - 1,
        i, find = self.pos, comparison;
    
    var idx;

    while (low <= high) {
        i = Math.floor((low + high) / 2);
        if (items[i].ts < find) { low = i + 1; continue; };
        if (items[i].ts > find) { high = i - 1; continue; };
        return i;
    }
    
    // no exact match, return the first lower value
    if (items[i].ts < find) { return i; }
    else { return i - 1; }
};

Flight.prototype.update = function () {
    log.trace('Flight.update()');
    
    var self = this;
    
    return self.init().then(function () {
        return self.migrateTo(self.items.length - 1);
    });
};
Flight.prototype.upBy = function (amount) {
    log.trace('Flight.upBy()', amount);
    
    var self = this;
    
    return self.init().then(function () {
        var destIdx = Math.min(self.idx + amount, self.items.length - 1);
        return self.migrateTo(destIdx);
    });
};
Flight.prototype.downBy = function (amount) {
    log.trace('Flight.downBy()', amount);
    
    var self = this;
    
    return self.init().then(function () {
        var destIdx = Math.max(self.idx - amount, -1);
        return self.migrateTo(destIdx);
    });
};
Flight.prototype.upTo =
Flight.prototype.downTo =
Flight.prototype.migrateTo = function (destIdx) {
    log.silly('Flight.migrateTo('+destIdx+')');
    
    var self = this;
    
    return self.init().then(function () {
        var items = self.items,
            curIdx = self.idx;
        
        if (items.length === 0) { return; }
        
        if (destIdx < -1 || destIdx >= items.length || destIdx !== destIdx|0) {
            throw new Error('Invalid destIdx: ' + destIdx);
        }
        
        if (curIdx === destIdx) { return; }
        
        if (curIdx > destIdx) {
            return self._migrateDown(curIdx, destIdx, items);
        }
        
        if (curIdx < destIdx) {
            // curIdx is either -1, or the last successful migration;
            // either way we don't want to run what it points to, but the
            // next one above, when moving up
            return self._migrateUp(curIdx + 1, destIdx, items);
        }
    }).then(function () {
        return self.storeVersion();
    });
};
Flight.prototype._migrateUp = Promise.method(function (idx, destIdx, items) {
    log.trace('Flight._migrateUp()', idx, destIdx);
    
    var self = this;
    
    if (idx > destIdx) { return; }
        
    var migration = items[idx];

    return Promise.try(function () {
        log.silly('Migrating up: ' + migration.name);
        if (!self.dry) {
            return migration.up();
        }
    })
    .then(function (res) {
        log.trace('OK.');
        if (!self.dry) {
            self.pos = migration.ts;
            self.idx = idx;
        }
    })
    .catch(function (err) {
        log.warn('Migration up to ' + migration.name + ' failed: ' + err);
        throw err;
    })
    .then(function () {
        return self._migrateUp(idx + 1, destIdx, items);
    });
});
Flight.prototype._migrateDown = Promise.method(function (idx, destIdx, items) {
    log.trace('Flight._migrateDown()', idx, destIdx);

    var self = this;
    
    if (idx <= destIdx) { return; }
        
    var migration = items[idx];

    return Promise.try(function () {
        log.silly('Migrating down: ' + migration.name);
        
        if (!self.dry) {
            return migration.down();
        }
    })
    .then(function (res) {
        log.trace('OK.');
        
        if (!self.dry) {
            self.pos = migration.ts;
            self.idx = idx - 1;
        }
    })
    .catch(function (err) {
        log.warn('Migration down to ' + migration.name + ' failed: ' + err);
        throw err;
    })
    .then(function () {
        return self._migrateDown(idx - 1, destIdx, items);
    });
});

Flight.prototype.loadFiles = function () {
    log.trace('Flight.loadFiles()');

    var self = this,
        path = self.path,
        mask = self.mask;
    
    return fs.readdir$(path)
    .catch(function (err) {
        if (err.cause && err.cause.code === 'ENOENT') {
            log.trace('No such directory: ' + path);
            return [ ];
        } else {
            throw err;
        }
    }).filter(function (fileName) {
        return mask.test(fileName);
    }).map(function (fileName) {
        var fullPath = PATH.join(path, fileName);
        return fs.stat$(fullPath).tap(function (stat) {
            stat.fullPath = fullPath;
            stat.fileName = fileName;
        });
    }).filter(function (stat) {
        return stat.isFile();
    }).map(function (stat) {
        log.trace('Found file: ' + PATH.basename(stat.fullPath));
        
        return new Migration({
            path: stat.fullPath,
            ts: stat.fileName.match(mask)[1]
        }, {
            log: log,
            knex: self.knex,
            Promise: Promise
        });
    }).then(function (migrations) {
        self.items.push.apply(self.items, migrations);
        return self.items.sort(function (a, b) { return a.ts - b.ts; });
    });
};

Flight.prototype.storeVersion = function () {
    if (this.dry) { return; }
    
    var self = this,
        knex = self.knex,
        version = self.pos;
    
    log.info('Ending position: ' + version);
    
    return knex('__flight').truncate()
    .then(function () {
        return knex.insert({ version: version }).into('__flight')
    })
    .catch(function (err) {
        log.warn('Error storing version in database: ' + err);
    });
}
Flight.prototype.end = function () {
    var self = this,
        knex = self.knex
    
    return this.storeVersion()
    .then(function () {
        return knex.client.pool.destroy();
    });
};

function Migration(opts, deps) {
    log.trace('new Migration()', opts);
    this.fullPath = opts.path;
    this.name = PATH.basename(opts.path, '.js');
    this.ts = opts.ts;
    var mod = require(opts.path)(deps);
    this.up = mod.up;
    this.down = mod.down;
    this.recover = mod.recover;
}
