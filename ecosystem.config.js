module.exports = {
    apps: [
        {
            name: 'Export report',
            script: 'service_export.js',
            instances: 2,
            exec_mode: 'cluster',
            node_args: '--max-old-space-size=8192 --expose-gc',
            max_memory_restart: '3072M', // Restart jika 1 proses > 3GB
            watch: false,
            env: {
                NODE_ENV: 'production',
            },
            log_date_format: 'YYYY-MM-DD HH:mm Z',
            error_file: './logs/err.log',
            out_file: './logs/out.log',
            merge_logs: true,
        },
    ],
};