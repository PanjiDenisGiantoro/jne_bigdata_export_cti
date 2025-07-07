const express = require('express');
const rateLimit = require('express-rate-limit');
const app = express();
const port = 3010;
const router = require('./routes');

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 menit
    max: 30, // Maksimum 100 permintaan per IP dalam 15 menit
    message: 'Terlalu banyak permintaan dari IP ini, coba lagi nanti.'
});

// Gunakan rate limiter di semua endpoint
app.use(limiter);

// Middleware
app.use(express.json());

// Gunakan router utama
app.use('/', router);

// Jalankan server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
