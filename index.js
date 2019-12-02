const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['localhost', 'h2'], localDataCenter: 'datacenter1', keyspace: 'timeserie' });

const insertQuery = 'INSERT INTO timeserie.sensors (sensor, day, ts, value)  VALUES (?, ?, ?, ?);';

const total = 10000;
var index = 0;

function insert(sensor, ts, value, cb){
  var day = `${ts.getDay()+1}-${ts.getMonth()+1}-${ts.getFullYear()}`
  client.execute(insertQuery, [ sensor, day, ts, value ], function(err, result) {
    cb(err, result)
  });
}

function rnd(min, max){
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
}

var cb = (err, result) => {
  if (err) {
    console.log('Fail to insert data')
  }else{
    index++;
    if (index == total){
      console.log('All done!')
    }else {
      if (index % 100 == 0){
        console.log(`Inserted ${index} from ${total}`)
      }
      insert(`sensor-${rnd(1,5)}`, new Date(), Math.random(), cb);
    }
  }
}

insert(`sensor-${rnd(1,5)}`, new Date(), Math.random(), cb)
