import "./App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import OurPicks from "./containers/OurPicks";
import Dashboard from "./containers/Dashboard";
import React, { useState, useEffect } from "react";
import HomePage from "./containers/HomePage";
import { ChakraProvider, Box } from "@chakra-ui/react";
import Signup from "./containers/Signup";
import Login from "./containers/Login";
import { supabase } from "./client";
import { AuthProvider } from "./Auth";
import ProtectedRoute from "./ProtectedRoute";

export const DataContext = React.createContext();

function App() {
  const [visData, setVisData] = useState([]);
  useEffect(() => {
    async function fetchData() {
      try {
        const response = await fetch(
          "http://ec2-18-139-116-71.ap-southeast-1.compute.amazonaws.com:5001/visualisation_data"
        );
        const data = await response.json();
        setVisData(data);
      } catch (err) {
        console.log("error in server");
        console.error(err.message);
      }
    }

    fetchData();
  }, []);

  //bgGradient={["linear(to-tr, teal.100, pink.200)"]}
  return (
    <ChakraProvider>
      <Box
        w="100%"
        h="100%"
        bgGradient={["linear(to-tr, white.100, white.200)"]}
      >
        <BrowserRouter>
          <Navbar />
          <DataContext.Provider value={{ visData }}>
            <Routes>
              <Route path="/" element={<HomePage visData={visData} />} />
              <Route path="/ourPicks" element={<OurPicks />} />
              <Route
                path="/dashboard"
                element={<Dashboard visData={visData} />}
              />
              <Route path="/login" element={<Login />} />
              <Route path="/signUp" element={<Signup />} />
            </Routes>
          </DataContext.Provider>
        </BrowserRouter>
      </Box>
    </ChakraProvider>
  );
}

// function HomePage(props) {
//   const [users, setUsers] = useState([]);

//   const [hello, setHello] = useState("hello");

//   useEffect(() => {
//     handleFetch();
//   }, []);

//   async function handleFetch() {
//     //e.preventDefault();
//     try {
//       const response = await fetch("http://localhost:5001/user_data");
//       // console.log(response.status);
//       // console.log(response.text());
//       const jsonData = await response.json();
//       console.log("jsonData: ", jsonData);

//       setUsers(jsonData);
//       console.log(users[0]);
//       console.log(users.username);

//       setHello(users.username);
//     } catch (err) {
//       console.log(err.message);
//     }
//   }

//   return (
//     <div>
//       <h1>Welcome to the homepage!</h1>
//       <button onClick={handleFetch}> click here</button>
//       {users && (
//         <div>
//           {" "}
//           <p> Username: {users.username} </p>{" "}
//         </div>
//       )}
//       <div> {hello}</div>
// <FetchData visData={props.visData} />
//     </div>
//   );
// }

export default App;
