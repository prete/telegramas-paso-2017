
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
        consoe.log('Insertando en db...');
        //store
        Promise.all([
            //los telegramas y errores se guardan cada 1000 en db
            //los restantes se guardan con esta llamada
            bulks.bulkTelegramas.execute(),
            bulks.bulkErrors.execute()
        ]).then(results => {
            console.info('Bulk insert result: ' + JSON.stringify(results[0]));
            console.info('Bulk Error insert result: ' + JSON.stringify(results[1]));
        }).catch(error => {
            logger.log('error', 'Error guardado telegramas en db.', error);
        }).finally(() => {
            if(db) db.close();
            console.info('Proceso finalizado');
        });
    });
});