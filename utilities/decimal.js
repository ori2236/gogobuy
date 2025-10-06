function roundTo(n, dp = 3) {
  return Number(Math.round(Number(n) * 10 ** dp) / 10 ** dp);
}

function addDec(a, b, dp = 3) {
  return roundTo(Number(a) + Number(b), dp);
}
function subDec(a, b, dp = 3) {
  return roundTo(Number(a) - Number(b), dp);
}
function mulMoney(qty, price, dp = 2) {
  return roundTo(Number(qty) * Number(price), dp);
}
function addMoney(a, b, dp = 2) {
  return roundTo(Number(a) + Number(b), dp);
}

module.exports = { roundTo, addDec, subDec, mulMoney, addMoney };
