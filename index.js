const osmosis = require('osmosis');
const iconv = require('iconv-lite');
const mongo = require('mongodb').MongoClient;
const winston = require('winston');
const _ = require('lodash');
const fs = require('fs');
const config = require('./config.js');

//inicializar logger para revisar eventos
const logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ name: 'error-file', filename: 'telegramas-error.log', level: 'error', timestamp: true }),
    ]
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


//conexion con mmongo
mongo.connect(config.mongo.url + config.mongo.db, function (err, db) {
    
    //inicializacion de bulk insert para guardar telegramas en db
    let bulk = db.collection(config.mongo.collection).initializeUnorderedBulkOp();
    
    //varaible para llevar registor de la mesa actual
    let currentMesa;

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
            //registrar mesa actual
            currentMesa = data;
        })
        //telegrama selector
        //las mesas no cargadas tiran error de #contentinfomesa not found
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
                'blancos': _.zipObject(telegrama.categorias, votosBlancos),
                'nulos': _.zipObject(telegrama.categorias, votosNulos),
                'recurridos': _.zipObject(telegrama.categorias, votosRecurridos),
                'impugnados': toNumber(telegrama.totales.impugnados),
                'detalle': _.map(telegrama.detalle, (voto) => {
                    return {
                        'partido': decodeString(voto.partido),
                        'lista': decodeString(voto.lista),
                        'votos': _.zipObject(telegrama.categorias, _.map(voto.votos, (v) => { return toNumber(v, -1) }))
                    };
                })
            };

            //bulk insert para acelerar el proceso de guardado en db
            bulk.insert(resultado);
            
            //guardar a disco
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
        })
        .error((err) => {
            //log de errores
            logger.log('error', err, currentMesa);
        })
        .done(() => { 
            //al finalizar el scraping ejecutar bulk insert
             bulk.execute(function(err, result) {
                 if (err) {
                     logger.log('error', 'Error guardado telegramas en db.', err);
                 }
                 logger.log('info', 'Bulk insert result: '+ result);
            });
            //cerrar conexion con mongo
             db.close();
            
            logger.log('info','Descarga de datos finalizada');
        });
});
