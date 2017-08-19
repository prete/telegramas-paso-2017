var config = {};

config.url = 'http://resultados.gob.ar/99/resu/content/telegramas/IPRO.htm';

config.mongo = {
    url: 'mongodb://localhost:27017/',
    db: 'PASO2017',
    collection: 'telegramas'
};

config.storeInFileSystem = false;

config.log = {
    fileLogLevel: 'error',
    consoleLogLevel: 'info',
    quiet: true
};

module.exports = config;