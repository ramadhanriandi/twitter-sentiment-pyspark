var express = require('express');
const db = require('../db');
var router = express.Router();

router.get('/', function (req, resp) {
    resp.sendFile('index.html');
});

router.get('/data', function (req, resp, next) {
    db.query('SELECT * FROM sentiments ORDER BY created_at DESC LIMIT 20', [], (err, res) => {
        if (err) {
            return next(err)
        }
        resp.json(res.rows);
    })
});

module.exports = router;
