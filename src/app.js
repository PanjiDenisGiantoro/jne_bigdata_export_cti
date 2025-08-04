const express = require('express');
const rateLimit = require('express-rate-limit');
const app = express();
const port = 3010;
const router = require('./routes');
const path = require('path');

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 menit
    max: 100, // Maksimum 100 permintaan per IP dalam 15 menit
    message: 'Terlalu banyak permintaan dari IP ini, coba lagi nanti.'
});

// Gunakan rate limiter di semua endpoint
app.use(limiter);

// Middleware
app.use(express.json());
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');
// Gunakan router utama
app.use('/', router);

// Jalankan server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
