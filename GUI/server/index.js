const express = require("express");
const pg = require("pg");
const app = express();
const cors = require("cors");
const pool = require("./db");

app.use(cors());
app.use(express.json());

// var connectionString =
//   "postgres://postgres:admin123@is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com:5432/spotify";
// var pgClient = new pg.Client(connectionString);
// pgClient.connect();

//ROUTES
app.get("/hello", (req, res) => {
  res.json("hello");
  console.log("hello");
});

//retrieve all users data
app.get("/user_data", async (req, res) => {
  try {
    const data = await pool.query("SELECT * FROM user_data");
    res.json(data.rows[0]);
    //console.log(data.rows[0]);
    //console.log("email: ", data.rows[0].email);
  } catch (err) {
    console.log("error in server");
    console.error(err.message);
  }
});

app.post("/spotifyUrl", async (req, res) => {
  const { username, url } = req.body;
  console.log(username, url);
  const result = await pool.query(
    "UPDATE user_data SET playlist_id = $1 WHERE username = $2",
    [url, username]
  );
  console.log(username, url);
  res.json({ message: "Url updated successfully" });
});

app.post("/login", async (req, res) => {
  const { username, password } = req.body;
  console.log(username, password);
  const result = await pool.query(
    "SELECT * FROM user_data WHERE username = $1 AND password = $2",
    [username, password]
  );

  const user = result.rows[0];
  console.log("user: ", user);

  if (!user) {
    //return res.status(401).json({ message: "invalid email or password!" });
    console.log("no user");
    return res.status(401).json({ message: "invalid email or password!" });
  } else {
    res.json({ message: "Login succesful" });
  }
});

app.get("/recommendations/:username", async (req, res) => {
  try {
    const username = req.params.username;
    const data = await pool.query(
      `SELECT * FROM recommendation_data WHERE username = '${username}'`
    );

    res.json(data.rows[0]);
    console.log(data.rows[0]);
    //console.log("email: ", data.rows[0].email);
  } catch (err) {
    console.log("error in server");
    console.error(err.message);
  }
});

app.get("/visualisation_data", async (req, res) => {
  try {
    const data = await pool.query("SELECT * FROM visualisation_data");
    res.json(data.rows[0]);
  } catch (err) {
    console.log("error in server");
    console.error(err.message);
  }
});

app.listen(5001, () => {
  console.log("Server has started at 5001");
});
