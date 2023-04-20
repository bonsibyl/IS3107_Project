import React, { useState, useEffect } from "react";

export default function FetchData() {
  
  const [users, setUsers] = useState([]);

  const [visData, setVisData] = useState([]);

  async function handleFetch() {
    //e.preventDefault();
    try {
      console.log("hello");
      //const response = await fetch("http://localhost:5001/user_data");
      const jsonData = await fetch("/user_data");
      //const jsonData = await response.json();
      console.log(jsonData);

      setUsers(jsonData);
      console.log(users);
    } catch (err) {
      console.log(err.message);
    }

    try {
      const response = await fetch("/visualisation_data");
      const data = await response.json(response[0]);
      console.log(data);

      setVisData(data);
      console.log(visData);
    }
    catch (err) {
          console.log("error in server");
          console.error(err.message);
    }
  }

  return (
    <div>
      <button onClick={handleFetch}> hello </button>
    </div>
  );
}

