var config = {};

config.url = 'http://resultados.gob.ar/99/resu/content/telegramas/IPRO.htm';

config.mongo = {
    url: 'mongodb://localhost:27017/',
    db: 'PASO2017',
    collection: 'telegramas'
};

config.storeInFileSystem = true;

module.exports = config;