# Node.js Project with Oracle, Redis, Bull Queue, and Excel Export

This is a Node.js application that integrates various technologies to process data, manage job queues, and generate reports in Excel format. The app connects to an Oracle database, uses Redis for job queue management, and utilizes Bull for efficient job queue processing. Additionally, the app supports downloading reports in Excel format and compressing them into zip files.

## Features
- **Oracle Database Connection**: Uses `oracledb` to connect the app with an Oracle database and fetch data.
- **Job Queue with Bull**: Manages long-running or repeated jobs using `Bull` job queues, with real-time job status monitoring using `bull-board`.
- **Excel Data Export**: Provides the ability to export data into Excel format using `ExcelJS`.
- **File Compression**: Supports combining and compressing files into `.zip` format using `archiver`.
- **Redis for Job Queue**: Uses `ioredis` to create an efficient connection to Redis.
- **Sentry Monitoring**: Integrates **Sentry** for error tracking and application profiling.
- **Clustering**: Supports **Node.js Clustering** to utilize multiple CPU cores for better performance.

## Prerequisites

Before running this app, make sure you have the following installed:

- Node.js (latest version recommended)
- Oracle Database Client (to connect to Oracle database)
- Redis server
- Bull Board (for job queue monitoring)
- Sentry (for error tracking and profiling)

### Install Dependencies

To get started, install the required dependencies by running:

```bash
npm install
