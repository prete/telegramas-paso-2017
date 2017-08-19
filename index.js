const osmosis = require('osmosis');
const iconv = require('iconv-lite');
const mongo = require('mongodb').MongoClient;
const winston = require('winston');
const _ = require('lodash');
const fs = require('fs');
const config = require('./config.js');

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

//conversion de string a numeros (valor por defecto 0)
function toNumber(number, defaultValue){
    var parsed = Number.parseInt(number);
    if(Number.isInteger(parsed)){
        return parsed;
    }else{
        return ((!defaultValue) ? 0 : defaultValue);
    }
}

//la pagina tiene encoding ISO-8859-1?
//se convierte a utf8 para evitar errores en acentos y ñ
function decodeString(string) {
    let encode1 = iconv.encode(string, 'latin1');
    let encode2 = iconv.encode(encode1, 'latin1');
    return iconv.decode(encode2, 'utf8');
}

console.log("Iniciando proceso. Sera notificado cada 1000 mesas. Para mas información deshabilitar config.log.quiet");

//conexion con mmongo
mongo.connect(config.mongo.url + config.mongo.db, function (err, db) {
    
    //inicializacion de bulk insert para guardar telegramas en db
    let bulk = db.collection(config.mongo.collection).initializeUnorderedBulkOp();
    let bulkErrors = db.collection("telegramasNoCargados").initializeUnorderedBulkOp();
    
    //varaible para llevar registor de Provicina/Seccion/Cirtuito/Mesa actual
    let currentPSCM;
    let counter = 0;

    //get de la URL base
    osmosis.get(config.url)
        .find('div.ulmes ul li a') // provincia selector
        .set('provincia')
        .follow('@href')
        .find('div.ulmes ul li a') //seccion selector
        .set('seccion')
        .follow('@href')
        .find('div.ulmes ul li a') //circuito selector
        .set('circuito')
        .follow('@href')
        .find('div.ulmes ul li a') //mesa selector
        .set('mesa')
        .follow('@href')        
        .then((context, data) => { 
            //registrar Provicina/Seccion/Cirtuito/Mesa actual
            currentPSCM = data;
        })
        //telegrama selector
        //las mesas no cargadas tiran error de #contentinfomesa not found
        //RANT: No se transcriben del PDF los campos "Cantidad de electores que han votado",
        //      "Cantidad de sobres en la urna" ni "Diferencia" 
        .find('#contentinfomesa')
        .set({
            'categorias': ['.pt1 .tablon thead th:skip(1)'],
            'totales': {
                'nulos': ['.pt1 .tablon tbody  tr:first  td'],
                'blancos': ['.pt1 .tablon tbody  tr:skip(1):first  td'],
                'recurridos': ['.pt1 .tablon tbody  tr:skip(2):first  td'],
                'impugnados': '.pt2 .tablon tbody tr:first td'
            },
            'detalle': [
                osmosis.find('#TVOTOS tbody tr:has(th.aladerecha)').set({
                    'partido': './preceding::th[@class="alaizquierda"][1]',
                    'lista': 'th',
                    'votos': ['td'],
                })
            ]
        })
        .data(telegrama => {
            //votos nulos/blancos/impugnados
            let votosNulos = _.map(telegrama.totales.nulos, toNumber);
            let votosBlancos = _.map(telegrama.totales.blancos, toNumber);
            let votosRecurridos = _.map(telegrama.totales.recurridos, toNumber);

            //procesar telegrama
            let resultado = {
                'provincia': telegrama.provincia,
                'seccion': telegrama.seccion,
                'circuito': telegrama.circuito,
                'mesa': telegrama.mesa,
                'blancos': {
                    'porCategoria': _.map(_.zipObject(telegrama.categorias, votosBlancos), (value, key) => ({
                        'categoria': key,
                        'votos': value
                    })),
                    'totales:': _.sum(votosBlancos)
                },
                'nulos': {
                    'porCategoria': _.map(_.zipObject(telegrama.categorias, votosNulos), (value, key) => ({
                        'categoria': key,
                        'votos': value
                    })),
                    'totales:': _.sum(votosNulos)
                },
                'recurridos': {
                    'porCategoria': _.map( _.zipObject(telegrama.categorias, votosRecurridos), (value, key) => ({
                        'categoria': key,
                        'votos': value
                    })),
                    'totales:': _.sum(votosNulos)
                },
                'impugnados': toNumber(telegrama.totales.impugnados),
                'detalle': _.map(telegrama.detalle, (voto) => {
                    return {
                        'partido': decodeString(voto.partido),
                        'lista': decodeString(voto.lista),
                        'votos': _.map( _.zipObject(telegrama.categorias, _.map(voto.votos, (v) => { return toNumber(v, -1) })), (value, key) => ({
                            'categoria': key,
                            'votos': value,
                        })),
                    };
                })
            };

            //bulk insert para acelerar el proceso de guardado en db
            bulk.insert(resultado);
            
            //guardar a disco (para habilitar modificar config.js)
            if (config.storeInFileSystem){
                fs.writeFile('./telegramas/' + resultado.mesa + '.json', JSON.stringify(resultado), function (err) {
                    //controlar errores en storage
                    if (err) {
                        logger.log('error', 'Error guardando telegrama en file system.', { error: err, raw: telegrama });
                    } else {
                        logger.log('info', 'Mesa' + resultado.mesa + ' OK.');
                    }
                });
            }
            
            //conteo de telegramas scrapeados
            counter++;
            if (counter % 1000 == 0) {
                console.info("Mesas scrapeadas: " + counter + "[~"+(counter/100000).toFixed(2)+"%]");
            }

        })
        .log(logger.debug)
        .error((err) => {
            //log de scrap errors
            logger.log('error', err, currentPSCM);
            bulkErrors.insert(currentPSCM);
        })
        .done(() => { 
            logger.log('info','Scrap de datos finalizado');

            //al finalizar el scraping ejecutar bulk insert
            bulk.execute(function(err, result) {
                if (err) {
                    logger.log('error', 'Error guardado telegramas en db.', err);
                }
                logger.log('info', 'Bulk insert result: ' + JSON.stringify(result));

                //bulk insert errors
                bulkErrors.execute(function(err, result) {
                    if (err) {
                        logger.log('error', 'Error guardado errores de telegramas en db.', err);
                    }
                    logger.log('info', 'Bulk Error insert result: ' + JSON.stringify(result));
                    //cerrar conexion con mongo
                    db.close();
                    
                    //guardado de datos finalizado
                    logger.log('info', 'Guardado de datos finalizado. Total', {'Total': counter});
                });
            });            
        });
});