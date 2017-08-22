const osmosis = require('osmosis');
const iconv = require('iconv-lite');
const _ = require('lodash');
const fs = require('fs');
const config = require('./config.js');
const Promise = require('bluebird');
const ProgressBar = require('progress');

//conversion de string a numeros (valor por defecto 0)
function toNumber(number, defaultValue) {
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

function releaseTheKraken(logger, db) {

    //inicializacion de bulk insert para guardar telegramas en db
    let bulkTelegramas = db.collection(config.mongo.successCollection).initializeUnorderedBulkOp();
    let bulkErrors = db.collection(config.mongo.errorCollection).initializeUnorderedBulkOp();    

    return new Promise((resolve, reject) => {
        //varaible para llevar registor de Provicina/Seccion/Cirtuito/Mesa actual
        let curentPSCM;
        //varaibles para mostrar avance
        let counter = 0;
        let errorCounter = 0;
        let successCounter = 0;
        let bar = new ProgressBar('Procesando [:bar] :percent [:current/:total] [@:rate/tps] [:etas]', {
            complete: '=',
            incomplete: ' ',
            width: 30,
            total: config.aproxTotalTelegramas
        });
                        
        //get de URL de resultados
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
                counter++;         
                //conteo de telegramas scrapeados
                bar.tick();
            })            
            //las mesas no cargadas tiran error de #contentinfomesa not found
            //RANT: No se transcriben del PDF los campos "Cantidad de electores que han votado",
            //      "Cantidad de sobres en la urna" ni "Diferencia" 
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
                        'porCategoria': _.zipWith(telegrama.categorias, votosBlancos, (categoria, votos) => {
                            return { 'categoria': categoria, 'votos': votos };
                        }),
                        'totales': _.sum(votosBlancos)
                    },
                    'nulos': {
                        'porCategoria': _.zipWith(telegrama.categorias, votosNulos, (categoria, votos) => {
                            return { 'categoria': categoria, 'votos': votos };
                        }),
                        'totales': _.sum(votosNulos)
                    },
                    'recurridos': {
                        'porCategoria': _.zipWith(telegrama.categorias, votosRecurridos, (categoria, votos) => {
                            return { 'categoria': categoria, 'votos': votos };
                        }),
                        'totales': _.sum(votosNulos)
                    },
                    'impugnados': toNumber(telegrama.totales.impugnados),
                    'detalle': _.map(telegrama.detalle, (voto) => {
                        return {
                            'partido': decodeString(voto.partido),
                            'lista': decodeString(voto.lista),
                            'totalesPorLista': _.sum(_.map(voto.votos, (v) => { return toNumber(v) })),
                            'votosPorCategoria': _.filter(
                                //agrupar votos por categoria
                                _.zipWith(telegrama.categorias, voto.votos, (categoria, votos) => {
                                    return { 'categoria': categoria, 'votos': toNumber(votos, -1) };
                                }),
                                //remover las categorias que no participan (marcadas con votos: -1)                            
                                (vc) => { return vc.votos != -1 }
                            )
                        };
                    })
                };

                //bulk insert para acelerar el proceso de guardado en db
                bulkTelegramas.insert(resultado);
                successCounter++;
                
                if (successCounter % 1000 == 0) { 
                    bulkTelegramas.execute().then(result => {
                        logger.log('info', 'Guardado batch de telegramas', result);
                    });
                    bulkTelegramas = db.collection(config.mongo.successCollection).initializeUnorderedBulkOp();
                }

                //guardar a disco (para habilitar modificar config.js)
                if (config.storage.enabled) {
                    fs.writeFile(config.storage.path + resultado.mesa + '.json', JSON.stringify(resultado), function (err) {
                        //controlar errores en storage
                        if (err) logger.log('error', 'Error guardando telegrama en file system.', { error: err, raw: telegrama });
                    });
                }
            })
            .log(logger.debug)
            .error((err) => {
                //log de scrap errors
                logger.log('error', err, currentPSCM);
                bulkErrors.insert(currentPSCM);
                errorCounter++;

                if (errorCounter % 1000 == 0) { 
                    bulkErrors.execute().then(result => {
                        logger.log('info', 'Guardado batch de telegramas fallidos', result);
                    });
                    bulkErrors = db.collection(config.mongo.errorCollection).initializeUnorderedBulkOp();
                }
            })
            .done(() => {
                bar.interrupt('Scrap de datos finalizado.');
                console.log('\n\n');
                //finalizar el scraping
                resolve({
                    'bulkTelegramas': bulkTelegramas,
                    'bulkErrors': bulkErrors
                });
            });
    });
};

module.exports = {
    start: releaseTheKraken
}