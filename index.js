
const winston = require('winston');
const config = require('./config.js');
const Promise = require('bluebird');
const mongo = require('mongodb').MongoClient;
const scraper = require('./scrap.js');

//inicializar logger para revisar eventos
const transports = [];
transports.push(
    new (winston.transports.File)({
        filename: 'telegramas.log',
        level: config.log.level,
        timestamp: true
    })
);

const logger = new (winston.Logger)({
    transports: transports
});

console.log("Iniciando scrap");
console.log("URL: "+config.url);

//conexion con mongo
const db = mongo.connect(config.mongo.url + config.mongo.db);

db.then(db => {

    //scrap
    scraper.start(logger, db).then((bulks) => {
        Promise.all(bulks).then((results) => {
            logger.log('info','Bulk results', results)
            //cerrar conexion
            db.close();
            console.info('Proceso finalizado');
        });
    }).catch((err) => { 
        logger.log('error', err);
    });
});