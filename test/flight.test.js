'use strict';

var should = require('should-eventually'),
    Flight = require('../index');

var PATH = require('path'),
    fs = require('fs');

var Knex = require('knex'),
    sqlite3 = require('sqlite3');

var baseDir = PATH.resolve(PATH.join(__dirname, '..'));

var Promise = require('bluebird');

Promise.promisifyAll(fs, { suffix: '$' });

describe('Flight', function () {
    describe('with no config', function () {
        var flight;

        it('should instantiate with no error', function () {
            flight = new Flight();
            return flight.init();
        });
        it('should end gracefully', function () {
            return flight.end();
        });
    });
    describe('fixture 1', function () {
        var dbPath = PATH.join(baseDir, 'test/fixtures/1'),
            dbFile = PATH.join(dbPath, 'sqlite3.db'),
            migrationPath = PATH.join(dbPath, 'migrations');
        
        var knex;
        
        before(function () {
            return fs.unlink$(dbFile)
            .catch(function (err) {
                if (err.cause && err.cause.code === 'ENOENT') {
                    return;
                }
                throw err;
            }).then(function () {
                knex = Knex({
                    client: 'sqlite3',
                    connection: { filename: dbFile }
                });
            
                return knex.schema.createTable('foo', function (table) {
                    table.increments('id');
                    table.string('thing');
                }).then(function () {
                    return knex.insert({ thing: 'bar' }).into('foo');
                });
            });
        });
        after(function () { return fs.unlink$(dbFile); });
        
        it('should not modify the database if \'dry\' is true', function () {
            var flight = new Flight({
                path: migrationPath,
                knex: knex,
                dry: true
            });
            
            return flight.update()
            .then(function () {
                return knex.schema.hasColumn('foo', 'bar')
                    .should.eventually.equal(false);
            });
        });
        
        describe('migrate up', function () {
            it('should add the \'bar\' column', function () {
                var flight = new Flight({
                    path: migrationPath,
                    knex: knex
                });
                
                return flight.update(knex)
                .then(function () {
                    return knex.schema.hasColumn('foo', 'bar')
                        .should.eventually.equal(true);
                });
            });
        });
        describe('migrate down', function () {
            it('should remove the \'bar\' column', function () {
                var flight = new Flight({
                    path: migrationPath,
                    knex: knex,
                    debug: true
                });
                
                return flight.downBy(1)
                .then(function () {
                    return knex.schema.hasColumn('foo', 'bar')
                        .should.eventually.equal(false);
                });
            });
        });
    });
    
});

function initDryRunDb(dbFile) {
    return fs.unlink$(dbFile)
    .catch(function (err) {
        if (err.cause && err.cause.code === 'ENOENT') {
            return;
        }
        throw err;
    })
    .then(function () {
        var db;
        
        return new Promise(function (resolve, reject) {
            db = new sqlite3.Database(dbFile, function (err) {
                if (err) { return reject(err); }
                Promise.promisifyAll(db, { suffix: '$' });
                resolve();
            });
        }).then(function () {
            return db.run$("CREATE TABLE foo(id INTEGER, thing VARCHAR)");
        }).then(function () {
            return db.run$("INSERT INTO foo (thing) VALUES ('bar')");
        }).then(function () {
            return db.close$();
        });
    });
}
