'use strict';
const fs = require('fs');
const {Pool} = require('pg');
const _ = require('lodash');
const extract = require('extract-zip')
const {basename, dirname} = require('path');
const async = require('async');
/*
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf && sudo sysctl -p

node app.js user=wireflies password=wireflies host=localhost port=5432 database=wireflies flightPath=~/school/tmp/secure.flightradar24.com/stockholm_1000km
*/
module.exports = {
    initDB: initDB,
    saveToDB: saveToDB
};

let pool;

function initDB(connectionString){
    console.log('Connecting to: ' + connectionString);
    pool = new Pool({
        connectionString: connectionString
    });

    let promises = [
        pool.query('CREATE TABLE IF NOT EXISTS flights ( flight_id bigint PRIMARY KEY,\
            aircraft_id bigint,\
            reg VARCHAR,\
            equip VARCHAR,\
            callsign VARCHAR,\
            flight VARCHAR,\
            schd_from VARCHAR,\
            schd_to VARCHAR,\
            real_to VARCHAR,\
            reserved VARCHAR)'),
        pool.query('CREATE TABLE IF NOT EXISTS flight_data (id bigserial PRIMARY KEY,\
            aircraft_id bigint,\
            snapshot_id bigint NOT NULL,\
            altitude int,\
            heading real,\
            latitude real,\
            longitude real,\
            radar_id int,\
            speed real,\
            squawk VARCHAR,\
            vert_speed real,\
            on_ground boolean)'),
    ];

    return Promise.all(promises);
}

function readZipFile(entry, zipfile){
    return new Promise((full, rej) => {
        zipfile.openReadStream(entry, (err, readStream) => {
            if(err){
                return rej(err);
            }

            let csvContents = '';
            readStream.on('data', (data) => {
                csvContents += data;
            });
            readStream.on('end', () => {
                const splitContents = csvContents.split('\n').splice(1).splice(0, -1);
                const data = _.map(splitContents, (dt) => {
                    //snapshot_id,altitude,heading,latitude,longitude,radar_id,speed,squawk
                    return {
                        snapshot_id: dt[0],
                        altitude: dt[1],
                        heading: dt[2],
                        latitude: dt[3],
                        longitude: dt[4],
                        radar_id: dt[5],
                        speed: dt[6],
                        squawk: dt[7]
                    };
                });

                full(data);
            });
        });
    });
}

function parseFlights(key, flightPath){
    return new Promise((full, rej) => {
        fs.readFile(flightPath, 'utf8', (err, content) => {
            if(err){
                return rej(err);
            }

            full(content);
        })
    }).then((content) => {
        //flight_id,aircraft_id,reg,equip,callsign,flight,schd_from,schd_to,real_to,reserved
        const splitContent = content.split('\n').splice(1).slice(0, -1);
        const flights = _.map(splitContent, (row, key) => {
            if(!row.split(',')[0]){
                console.log('There was no flight id, row: ##' + row + '##' + ' key: ' + key + ', max: ' + splitContent.length);
            }
            return {
                flight_id: row.split(',')[0],
                aircraft_id: row.split(',')[1],
                reg: row.split(',')[2],
                equip: row.split(',')[3],
                callsign: row.split(',')[4],
                flight: row.split(',')[5],
                schd_from: row.split(',')[6],
                schd_to: row.split(',')[7],
                real_to: row.split(',')[8],
                reserved: undefined,
                data: []
            };
        });

        let flightsObject = {};
        _.forEach(flights, (flight) => {
            flightsObject[flight.flight_id] = flight;
        });

        return new Promise((full) => full(flightsObject));
    });
}

function saveToDB(flightFiles){
    console.log('Saving to db...');
    return new Promise((full, rej) => {
        
        let n = Object.keys(flightFiles.flight).length;
        let i = 0;
        async.forEachOfLimit(flightFiles.flight, 1, (flightFile, key, callback) => {
            const dirn = dirname(flightFiles.data[key]);
            const basen = basename(flightFiles.data[key]);
            parseFlights(key, flightFiles.flight[key]).then((flights) => {
                extract(flightFiles.data[key], {dir: process.cwd() + '/tmp', onEntry: (entry, zipfile) => {
                    const flightId = basename(entry.fileName, '.csv').split('_')[1];
                    readZipFile(entry, zipfile).then((flightData) => {
                        zipfile.close();
                        flights[flightId].data = flightData;
                    }).catch((err) => {
                        callback(err);
                    });
                    
                }}, (err) => {
                    if(err){
                        return callback(err);
                    }
    
                    //Save flights to postgresql
                    saveFlights(flights).then(() => callback()).catch(callback);
                    i++;
                    console.log(i + ' of ' + n);
                    if(i % (n/100) === 0){
                        console.log((i/n) + '% done');
                    }
                });
            }).catch((err) => callback(err));
        }, (err) => {
            if(err){
                return rej(err);
            }

            full();
        });
    });
}

/*
    id,aircraft_id,snapshot_id,altitude,heading,latitude,longitude,radar_id,speed,squawk,vert_speed,on_ground
*/
function saveFlights(flights){
    console.log('Saving flights csv');
    let promises = _.map(flights, (flight) => {
        return pool.query('INSERT INTO flights(flight_id,aircraft_id,reg,equip,callsign,flight,schd_from,schd_to,real_to,reserved) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [flight.flight_id, flight.aircraft_id, flight.reg, 
            flight.equip, flight.callsign, flight.flight, flight.schd_from, flight.schd_to, flight.real_to, flight.reserved]);
    });

    _.forEach(flights, (flight) => {
        promises.push(_.map(flights.datapoints, (datapoint) => {
            return pool.query('INSERT INTO flight_data(id,aircraft_id,snapshot_id,altitude,heading,latitude,longitude,radar_id,speed,squawk,vert_speed,on_ground) VALUES(NULL,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)', [flight.aircraft_id, datapoint.snapshot_id,
            datapoint.altitude, datapoint.heading, datapoint.latitude, datapoint.longitude, datapoint.radar_id, datapoint.speed,
            datapoint.squawk, datapoint.vert_speed, datapoint.on_ground]);
        }));
    });

    return Promise.all(promises);
}