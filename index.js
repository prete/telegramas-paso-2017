const osmosis = require('osmosis');
const iconv = require('iconv-lite');
const mongo = require('mongodb').MongoClient;
const winston = require('winston');
const _ = require('lodash');
const fs = require('fs');
const config = require('./config.js');

const logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      //new (winston.transports.File)({ name: 'info-file', filename: 'telegramas-info.log', level: 'info' , timestamp: true}),
      //new (winston.transports.File)({ name: 'error-file', filename: 'telegramas-error.log', level: 'error', timestamp: true }),
    ]
});

function toNumber(number, defaultValue){
    var parsed = Number.parseInt(number);
    if(Number.isInteger(parsed)){
        return parsed;
    }else{
        return ((!defaultValue) ? 0 : defaultValue);
    }
}

function decodeString(string) {
    //don't ask why double encode then decode, it just works
    let encode1 = iconv.encode(string, 'latin1');
    let encode2 = iconv.encode(encode1, 'latin1');
    return iconv.decode(encode2, 'utf8');
}


mongo.connect(config.mongo.url + config.mongo.db, function (err, db) {
    
    let bulk = db.collection(config.mongo.collection).initializeUnorderedBulkOp();

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
            logger.log('info', 'Accediendo a telegrama', data);
        })
        .find('#contentinfomesa') //telegrama selector    
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
            let votosNulos = _.map(telegrama.totales.nulos, toNumber);
            let votosBlancos = _.map(telegrama.totales.blancos, toNumber);
            let votosRecurridos = _.map(telegrama.totales.recurridos, toNumber);

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

            bulk.insert(resultado);
            
            if (config.storeInFileSystem){
                fs.writeFile('./telegramas/' + resultado.mesa + '.json', JSON.stringify(resultado), function (err) {
                    if (err) {
                        logger.log('error', 'Error guardando telegrama en file system...', { error: err, raw: telegrama });
                    } else {
                        logger.info('[FS] Mesa' + resultado.mesa + ' OK.');
                    }
                });
            }    
        })
        .log(logger.debug)
        .error(logger.error)
        .done(() => { 
             bulk.execute(function(err, result) {
                 if (err) logger.error(err);
                 logger.info(result);
            });
            db.close();
            logger.info('Descarga de datos finalizada');
        });

});
