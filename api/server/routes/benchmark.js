const express = require('express');
const router = express.Router();
const BenchmarkController = require('../controllers/BenchmarkController');
const requireJwtAuth = require('../middleware/requireJwtAuth');
const checkAdmin = require('../middleware/roles/admin');

router.use(requireJwtAuth);
router.use(checkAdmin);

router.post('/run', BenchmarkController.runBenchmark);
router.get('/task/:taskId', BenchmarkController.getTaskStatus);
router.get('/result/:taskId', BenchmarkController.getResult);
router.get('/sql-comparison/:taskId', BenchmarkController.getSQLComparison);
router.get('/tasks', BenchmarkController.listTasks);

module.exports = router;
