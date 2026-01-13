const addImages = require('./addImages');
const addCharts = require('./addCharts');
const handleOutputs = require('./handleOutputs');

module.exports = {
  addImages,
  addCharts,
  ...handleOutputs,
};
