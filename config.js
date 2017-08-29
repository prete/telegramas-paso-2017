var config = {};

config.url = 'http://resultados.gob.ar/99/resu/content/telegramas/IPRO.htm';

config.mongo = {
    url: 'mongodb://localhost:27017/',
    db: 'PASO2017',
    successCollection: 'telegramas',
    errorCollection: 'telegramasNoCargados'
};

config.storage = {
    enabled: false,
    path: './telegramas'
}

config.log = {
    level: 'info'
};

//Segun Gustavo Mason (subdirector de la Cámara Nacional Electoral)
config.aproxTotalTelegramas = 98084;

module.exports = config;