'use strict';
const fs = require('fs');
const _ = require('lodash');
const walk = require('walk');
const async = require('async');
const {basename, extname} = require('path');

module.exports = {
    readAllCSV: readAllCSV
};

function readAllCSV(path, callback){
    callback = _.once(callback);
    let walker = walk.walk(path);
    let files = {};
    let flightFiles = {};
    let flightDatafiles = {};

    walker.on('file', (root, stat, next) => {
        /*fs.readFile(root + '/' + stat.name, 'utf8', (err, data) => {
            if(err){
                return callback(err);
            }
            files[stat.name] = parseCSV(data);
            next();
        });*/
        const _extname = extname(stat.name);
        if(_extname === '.csv'){
            flightFiles[basename(stat.name, '.csv').split('_')[0]] = root + '/' + stat.name;
        }else if(_extname == '.zip'){
            flightDatafiles[basename(stat.name, '.zip').split('_')[0]] = root + '/' + stat.name;
        }

        next();
    });
    walker.on('end', () => {
        callback(null, {flight: flightFiles, data: flightDatafiles});
    });
}

function parseCSV(data){
    return data;
}