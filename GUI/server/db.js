const Pool = require("pg").Pool;
const AWS = require("aws-sdk");

const credentials = new AWS.Credentials({
  accessKeyId: "AKIAY73PMJYUU6QFJTOP",
  secretAccessKey: "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",
});

AWS.config.update({
  credentials: credentials,
  region: "ap-southeast-1",
});

const pool = new Pool({
  user: "postgres",
  password: "admin123",
  //host: "http://is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com",
  host: "is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com",
  port: 5432,
  database: "spotify",
  ssl: { rejectUnauthorized: false },
  credentials: credentials,
});

module.exports = pool;
