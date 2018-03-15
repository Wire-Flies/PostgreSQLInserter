'use strict';
const fs = require('fs');
const path = require('path');
const pg = require('pg');
const rimraf = require('rimraf');
const unzip = require('unzip');
const _ = require('lodash');
const Pool = pg.Pool;
const FAIL_HARD = false;
const TEMP_DIR = './tmp';
const MAX_CLIENTS = 20;

let pool;
let argv = {};
_.forEach(process.argv, (arg) => {
    if(arg.split('=').length < 2){
        return;
    }
    argv[arg.split('=')[0]] = arg.split('=')[1];
});

let doExit = false;
const requiredArgv = ['flightPath', 'host', 'user', 'database', 'port', 'password'];
_.forEach(requiredArgv, (env) => {
    if(!argv[env] && argv[env] !== ''){
        console.log(env + ' must be set');
        doExit = true;
    }
});

console.log(argv);
console.log(process.argv);

const flightPath = argv.flightPath;
const dbHost = argv.host;
const dbUser = argv.user;
const dbDB = argv.database;
const dbPort = argv.port;
const dbPassword = argv.password;
//postgresql://dbuser:secretpassword@database.server.com:3211/mydb
const connectionString = 'postgresql://' + dbUser + ':' + dbPassword + '@' + dbHost + ':' + dbPort + '/' + dbDB;

if(doExit){
    process.exit(-1);
}
async function runAsync(){
    await refreshTemp();
    await initDB(connectionString);
    const foundCSV = fromDir(flightPath, '.csv');
    const foundZIP = fromDir(flightPath, '.zip');

    let startTime = new Date().getTime();
    let i = 0;
    for(const csv of foundCSV){
        i++;
        const res = await insertFlights('', csv);
        //console.log('res ', res);
        if(i % 10 === 0){
            console.log('Processed ' + i + ' flights, total allocated time: ' + (new Date().getTime() - startTime) / 1000);
        }
    }

    for(const zip of foundZIP){
        await processZip(zip);
    }
}

runAsync();

async function refreshTemp(){
    await rmTemp();
    await createTemp();
}

async function createTemp(){
    return new Promise((full, rej) => {
        console.log('createtemp');
        fs.mkdir(TEMP_DIR, (err) => {
            console.log('fs.mkdir done');
            if(err){
                console.log('ERRR: ', err);
                return rej(err);
            }
            full();
        });
    })
}

async function rmTemp(){
    return new Promise((full, rej) => {
        console.log('rmtemp');
        rimraf(TEMP_DIR, (err) => {
            console.log('rimraf done');
            if(err){
                console.log('ERR: ', err);
                return rej(err);
            }
            full();
        });
    });
}

function fromDir(startPath,filter){
    if (!fs.existsSync(startPath)){
        return [];
    }

    let ret = [];
    var files=fs.readdirSync(startPath);
    for(var i=0;i<files.length;i++){
        var filename=path.join(startPath,files[i]);
        var stat = fs.lstatSync(filename);
        if (stat.isDirectory()){
            ret = ret.concat(fromDir(filename,filter)); //recurse
        }
        else if (filename.indexOf(filter)>=0) {
            ret.push(filename);
        };
    };

    return ret;
};

async function processZip(zipPath){
    await refreshTemp();
    await unzipZip(zipPath);
    const files = fromDir(TEMP_DIR, '.csv');
    console.log('UNZIPPED: ',files);
    for(const file of files){
        const result = await processZipCSV(file);
    }
}

async function unzipZip(zipPath){
    console.log('unzipZip: "' + zipPath + '"');
    let stream = fs.createReadStream(zipPath);

    console.log('unzipping');
    return new Promise((full) => {
        console.log('promize, zippath: ' + zipPath);
        stream.on('close', full);
        stream.on('error', (err) => console.log('THERE WAS AN ERROR STREAMING UNZIPPED FILE: ', err));
        stream.pipe(unzip.Extract({ path: TEMP_DIR }));
    });
}

async function processZipCSV(filePath){
    const flightId = path.basename(filePath, '.csv').split('_')[1];
    return new Promise((full, rej) => {
        fs.readFile(filePath, 'utf8', (err, content) => {
            if(err){
                return rej(err);
            }

            full(content);
        });
    }).then((content) => {
        const splitContent = content.split('\n').slice(1).slice(0,-1);
        const flightInserts = _.map(splitContent, (row, key) => {
            if(!row.split(',')[0]){
                console.log('There was no flight id');
            }

            row = row.split(',');
            return pool.query('INSERT INTO flight_data(aircraft_id,snapshot_id,altitude,heading,latitude,longitude,radar_id,speed,squawk) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)', [
                flightId, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7] 
            ]);
        });

        return Promise.all(flightInserts);
    });
}

function saveFlights(flights){
    console.log('Saving flights csv');
    let promises = _.map(flights, (flight) => {
        
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


async function insertFlights(key, flightPath){
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
                reserved: undefined
            };
        });

        const insertPromises = _.map(flights, (flight) => {
            return pool.query('INSERT INTO flights(flight_id,aircraft_id,reg,equip,callsign,flight,schd_from,schd_to,real_to,reserved) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [flight.flight_id, flight.aircraft_id, flight.reg, 
                flight.equip, flight.callsign, flight.flight, flight.schd_from, flight.schd_to, flight.real_to, flight.reserved]);
        });

        return Promise.all(insertPromises).then(() => {
            return new Promise((full) => full());
        }).catch((err) => {
            console.log(err);
            if(FAIL_HARD){
                return new Promise((full,rej) => rej(err));
            }
            return new Promise((full) => full());
        });
    });
}

async function initDB(connectionString){
    console.log('Connecting to: ' + connectionString);
    pool = new Pool({
        connectionString: connectionString,
        max: MAX_CLIENTS
    });
    return pool.query('DROP TABLE IF EXISTS flights').then(() => {
        return pool.query('DROP TABLE IF EXISTS flight_data');
    }).then(() => {
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
    });
}
