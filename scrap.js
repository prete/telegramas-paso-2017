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

    return new Promise((resolve, reject) => {

        //inicializacion de bulk insert para guardar telegramas en db
        let bulk = {
            telegramas: db.collection(config.mongo.successCollection).initializeUnorderedBulkOp(),
            errors: db.collection(config.mongo.errorCollection).initializeUnorderedBulkOp()/*,
            debug: db.collection("followedURLs").initializeUnorderedBulkOp()*/
        };
        
        //varaible para llevar registor de Provicina/Seccion/Cirtuito/Mesa actual
        let curentPSCM;
        
        //varaibles para mostrar avance
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
            .set({ 'href':'@href'})
            .then((context, data) => {
                //registrar Provicina/Seccion/Cirtuito/Mesa actual
                currentPSCM = data; 
                //registro
                //bulk.debug.insert(data);
                logger.log('info', "link data", data);
                //conteo de telegramas scrapeados
                bar.tick();
            })
            .follow('@href')
            //las mesas no cargadas tiran error de #contentinfomesa not found
            //RANT: No se transcriben del PDF los campos "Cantidad de electores que han votado",
            //      "Cantidad de sobres en la urna" ni "Diferencia" 
            .find('#contentinfomesa') //telegrama selector
            .set({
                'href': '@href',
                'estado': '.altreinta table tbody tr:last td',
                //RANT: No se transcriben del PDF las categorias municipales
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
            .data((telegrama) => {
                //votos nulos/blancos/impugnados
                let votosNulos = _.map(telegrama.totales.nulos, toNumber);
                let votosBlancos = _.map(telegrama.totales.blancos, toNumber);
                let votosRecurridos = _.map(telegrama.totales.recurridos, toNumber);

                //procesar telegrama
                let resultado = {
                    'estado': telegrama.estado,
                    'href': 'http://resultados.gob.ar/99/resu/content/telegramas/'+telegrama.href,
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
                bulk.telegramas.insert(resultado);
                
                //guardar a disco (para habilitar modificar config.js)
                if (config.storage.enabled) {
                    fs.writeFile(config.storage.path + resultado.mesa + '.json', JSON.stringify(resultado), function (err) {
                        //controlar errores en storage
                        if (err) logger.log('error', 'Error guardando telegrama en file system.', { error: err, raw: telegrama });
                    });
                }
            })
            //.log(logger.debug)
            .error((err) => {
                //log de scrap errors
                logger.log('error', err, currentPSCM);
                bulk.errors.insert(currentPSCM);
            })
            .done(() => {
                console.log('Scrap de datos finalizado.');
 
                console.log('Insertando en db...');
                //store telegramas y errores se guardan con esta llamada
                let executed = [];
                executed.push(new Promise((resolve, reject) => {
                    bulk.telegramas.execute((err, result) => {
                        resolve(result);
                    })
                }));
                executed.push(new Promise((resolve, reject) => {
                    bulk.errors.execute((err, result) => {
                        resolve(result);
                    })
                }));/*
                executed.push(new Promise((resolve, reject) => {
                    bulk.debug.execute((err, result) => {
                        resolve(result);
                    })
                }));*/

                //finalizar el scraping
                resolve(executed);
            });
    });
};

module.exports = {
    start: releaseTheKraken
}