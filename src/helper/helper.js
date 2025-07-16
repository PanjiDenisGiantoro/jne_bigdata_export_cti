const { reportQueues } = require('../queue/jobQueue');

async function getQueueData() {
    const queueData = [];

    for (const queue of reportQueues) {
        const counts = await queue.getJobCounts();
        const jobs = await queue.getJobs(
            ['active', 'waiting', 'completed', 'failed', 'delayed'],
            0, 0, false
        );

        const last = jobs?.[0] ?? null;
        let lastUpdated = null;
        let lastJobInfo = null;

        if (last) {
            const timestamp = last.finishedOn || last.processedOn || last.timestamp;
            lastUpdated = formatDateTime(timestamp);

            lastJobInfo = {
                id: last.id,
                name: last.data?.type && last.data?.user_id
                    ? `${last.data.type} - ${last.data.user_id}`
                    : '(no name)',
                status: last.finishedOn ? 'completed' :
                        last.failedReason ? 'failed' :
                        last.processedOn ? 'active' :
                        last.delay ? 'delayed' : 'waiting',
                duration: last.finishedOn && last.processedOn
                    ? formatDuration(last.finishedOn - last.processedOn)
                    : null
            };
        }

        queueData.push({
            name: queue.name,
            waiting: counts.waiting,
            active: counts.active,
            completed: counts.completed,
            failed: counts.failed,
            delayed: counts.delayed,
            lastUpdated,
            lastJob: lastJobInfo,
        });
    }

    // Native Date grouping
    const now = Date.now();
    const grouped = {
        'Active Recently (< 1h)': [],
        'Inactive (> 1h)': [],
        'Never Active': []
    };

    for (const q of queueData) {
        if (!q.lastUpdated) {
            grouped['Never Active'].push(q);
        } else {
            const lastTime = new Date(q.lastUpdated).getTime();
            const diffMinutes = (now - lastTime) / (1000 * 60);
            if (diffMinutes < 60) {
                grouped['Active Recently (< 1h)'].push(q);
            } else {
                grouped['Inactive (> 1h)'].push(q);
            }
        }
    }

    return grouped;
}

// ✅ Format to "YYYY-MM-DD HH:mm:ss"
function formatDateTime(timestamp) {
    const date = new Date(timestamp);
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mi = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

// ✅ Format milliseconds to h m s
function formatDuration(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    let result = '';
    if (hours > 0) result += `${hours}h `;
    if (minutes > 0) result += `${minutes}m `;
    result += `${seconds}s`;
    return result.trim();
}

module.exports = {
    getQueueData
};
