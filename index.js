
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
        level: config.log.fileLogLevel,
        timestamp: true
    })
);
if (!config.log.quiet) {
  transports.push(    
      new (winston.transports.Console)({
          level: config.log.consoleLogLevel,
          timestamp: true
      })
    );  
}
const logger = new (winston.Logger)({
    transports: transports
});

console.log("Iniciando scrap");
console.log("URL: "+config.url);

//conexion con mongo
const db = mongo.connect(config.mongo.url + config.mongo.db);

db.then(db => {
    //inicializacion de bulk insert para guardar telegramas en db
    let bulkTelegramas = db.collection(config.mongo.successCollection).initializeUnorderedBulkOp();
    let bulkErrors = db.collection(config.mongo.errorCollection).initializeUnorderedBulkOp();
    
    //scrap
    scraper.start(logger, bulkTelegramas, bulkErrors).then(() => {
        consoe.log('Insertando en db...');
        //store
        Promise.all([
            // inserta los telegramas    
            bulkTelegramas.execute(),
            // inserta los telegramas con error (no cargados)
            bulkErrors.execute()
        ]).then(results => {
            logger.log('info', 'Bulk insert result: ' + JSON.stringify(results[0]));
            logger.log('info', 'Bulk Error insert result: ' + JSON.stringify(results[1]));
        }).catch(error => {
            logger.log('error', 'Error guardado telegramas en db.', error);
            }).finally(() => {
            if(db) db.close();
            logger.log('info', 'Proceso finalizado');
        });
    });
});