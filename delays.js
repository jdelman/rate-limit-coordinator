/**
we know:

 - N = number of nodes
 - tasksComplete = total number of completed tasks
 - tStart = time we start
 - tNow = current time
 - tAvg = average time it takes to send
 - tTotal = average time it takes to send + delay
 - delayCurrent = current delay value
 - delayMin = minimum delay
 - rateMax = the most we can send (fixed)

we want to find delayNext, i.e. what do we set the delay to next time such that
rate is below rateMax within some margin?
**/

function getDelayNextNaive({ delayCurrent, delayMin, tasksComplete, tElapsed, rateMax }) {
  /**
    get rate, if exceeded, increase delay by 50ms
    if under, decrease delay by 10ms
    this is naive because it doesn't try to optimize the rate based
    on the number of nodes, it just makes small adjustments
   */

  let delayNext;
  const rateCurrent = tasksComplete / tElapsed;
  if (rateCurrent > rateMax * .9) {
    delayNext = delayCurrent + 50;
  }
  else if (rateCurrent < rateMax * .9) {
    delayNext = delayCurrent - 10;
    if (delayNext < delayMin) {
      delayNext = delayMin;
    }
  }

  return delayNext;
}

module.exports = {
  getDelayNextNaive,
};