const bluebird = require('bluebird');
const redis = require('redis');
const d = require('./delays');
bluebird.promisifyAll(redis);

const COORDINATOR_INTERVAL = 5000;
const PRODUCER_CHECK_INTERVAL = 10000;
const TARGET_TASKS_PER_MS = 0.25;
const MIN_DELAY = 1 / TARGET_TASKS_PER_MS;
const CONCURRENCY = 100;

const _client = redis.createClient();
const pause = timeout => new Promise(resolve => setTimeout(resolve, timeout));
const apiCall = () => {
  // sleep for 1-100 ms
  const sleepFor = Math.round(Math.random() * 100);
  return pause(sleepFor);
};

// one node to manage
async function coordinator() {
  const client = _client.duplicate();

  const setDelay = getSetDelay(client);

  // check the list of running clients, and set the rate
  setInterval(() => {
    checkListOfRunningClients(client);
  }, COORDINATOR_INTERVAL);

  setInterval(setDelay, 1000);
}

async function producer() {
  const client = _client.duplicate();
  const myName = Math.round(Math.random() * 100000);
  const key = `node:${ myName }`;

  // ensure I'm registered as a member every 10 seconds
  setInterval(async function() {
    await Promise.all([
      client.saddAsync('client_list', key),
      client.psetexAsync(key, PRODUCER_CHECK_INTERVAL + 1000, 'true') // give a little leeway if we restart or something
    ]).catch(err => console.error('error:', err))
  }, PRODUCER_CHECK_INTERVAL);

  // fake queue
  const queue = Array(1000000).fill().map((_, i) => i);

  while (queue.length) {
    let taskTime = Date.now();

    // peel a certain amount of jobs off the queue
    const nextJobs = queue.splice(0, CONCURRENCY);

    // run and measure task
    await Promise.all(nextJobs.map(apiCall));
    taskTime = taskTime - Date.now();

    // increase the counter and then wait
    await client.incrbyAsync('tasks_complete', nextJobs.length);

    const delay = await client.getAsync('delay');

    // console.log('pausing for', delay);
    await pause(delay);
  }
}

async function checkListOfRunningClients(client) {
  // aka the "zookeeper" algorithm

  const members = await client.smembersAsync('client_list');
  // make sure they're all still alive
  let aliveClients = 0;
  for (const member of members) {
    const memberKey = `node:${ member }`;
    const isAlive = await client.getAsync(memberKey);
    if (isAlive) {
      aliveClients++;
    }
    else {
      // remove from the list
      await client.sremAsync('client_list', memberKey);
    }
  }

  // set the total
  client.set('clients_alive', aliveClients);
}

function getSetDelay(client) {
  const totalStart = Date.now();
  let segmentStart = totalStart;

  // set a default delay
  client.setAsync('delay', '500');

  return async function setRate() {
    const [
      tasksComplete,
      delayCurrent
    ] = await Promise.all([
      client.getAsync('tasks_complete'),
      client.getAsync('delay').then(delay => Number(delay))
    ]);

    const tElapsed = Date.now() - segmentStart;
    const delayNext = d.getDelayNextNaive({
      tElapsed,
      tasksComplete,
      delayCurrent,
      delayMin: MIN_DELAY,
      rateMax: TARGET_TASKS_PER_MS
    });

    console.log('totalElapsed=', Date.now() - totalStart, 'rate=', tasksComplete / tElapsed, 'rpms', 'delayNext=', delayNext);

    client.setAsync('delay', delayNext);

    // reset the window
    segmentStart = Date.now();
    client.setAsync('tasks_complete', 0);
  }
}

//////////////////////////////////////////////////////////////////////////////

// start one coordinator and multiple producers
const { Worker, isMainThread } = require('worker_threads');

if (isMainThread) {
  console.log('starting coordinator');
  coordinator().catch(err => {
    console.error('coordinator error:', err);
  });

  // spawn N producers
  for (let i = 0; i < 10; i++) {
    console.log('spawning thread', i);
    const worker = new Worker(__filename);
  }
}
else {
  console.log('running producer');
  producer().catch(err => {
    console.error('producer error:', err);
  });
}
