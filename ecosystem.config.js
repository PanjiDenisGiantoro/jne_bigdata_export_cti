module.exports = {
    apps: [
        {
            name: 'Export report',
            script: 'src/app.js',
            instances: 2,
            exec_mode: 'cluster',
            node_args: '--max-old-space-size=28672 --expose-gc',
            max_memory_restart: '28672M',
            watch: false,
            env: {
                NODE_ENV: 'production',
            },
            log_date_format: 'YYYY-MM-DD HH:mm Z',
            error_file: './src/log/err.log',
            out_file: './src/log/out.log',
            merge_logs: true,
        },
    ],
};