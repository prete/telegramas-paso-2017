# what be this?
Scraper for the telegrams from the Argentine PASO Elections on 13th of August of 2017.
This crawls through the transcribed telegrams at http://resultados.gob.ar/

# technically
Scraper using node-osmosis (web scraper for node.js). Results are stored in mongodb. 

# can i haz results?
Sure, go to the releases to get the db dump.

# what can i do with this?
```javascript
// see how many telegrams where transcribed in correct state
db.telegramas.aggregate([
  {
    $group: 
    {
      "_id": "$estado",
      "total": { $sum: 1 }
    }
  }
]);
```

```javascript
// get total invalid/protest votes by province
db.telegramas.aggregate( [
  {
    $match: { "estado": "Grabada" }
  },
  {
    $group: 
    {
      "_id": "$provincia",
      "blancos": {$sum: "$blancos.totales"},
      "nulos": {$sum: "$nulos.totales"},
      "recurridos": {$sum: "$recurridos.totales"},
      "impugnados": {$sum: "$impugnados"},
    }
  }
]);
```

you get the gist...

