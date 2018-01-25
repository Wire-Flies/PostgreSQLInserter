'use strict';
//const argv = require('yargs').argv;
const fileReader = require('./fileReader.js');
const {initDB, saveToDB} = require('./insertDB.js');
const _ = require('lodash');

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

fileReader.readAllCSV(argv.flightPath, (err, flights) => {
    if(err){
        console.log('There was an error 1');
        console.log(err);
        return process.exit(-1);
    }
    //console.log(flights);
    
    initDB(connectionString).then(() => {
        //return new Promise((full) => full());
        return saveToDB(flights);
    }).then(() => {
        console.log('Done saving to database');
    }).catch((dbErr) => {
        console.log('There was an error 2');
        console.log(dbErr);
        process.exit(-1);
    });
});