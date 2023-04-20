import React from "react";
import {
  Flex,
  Box,
  Td,
  TableContainer,
  Table,
  TableCaption,
  Thead,
  Tr,
  Stack,
  Button,
  Heading,
  Text,
  Tbody,
  Tfoot,
  Th,
} from "@chakra-ui/react";
import { useState, useRef, useEffect } from "react";
import { ViewIcon, ViewOffIcon } from "@chakra-ui/icons";
import { json, useNavigate } from "react-router-dom";

function OurPicks() {
  const [recommendations, setRecommendations] = useState([]);
  const [recExplanations, setRecExplanations] = useState([]);
  const [songLinks, setSongLinks] = useState([]);

  useEffect(() => {
    handleFetch();
  }, []);

  async function handleFetch() {
    const username = localStorage.getItem("user");
    if (username != null) {
      try {
        const response = await fetch(
          `http://localhost:5001/recommendations/${username}`
        );
        const jsonData = await response.json();
        console.log("jsonData: ", jsonData);

        setRecommendations(jsonData.recommendation);
        setRecExplanations(jsonData.rec_explanation);
        setSongLinks(jsonData.rec_links);
      } catch (err) {
        console.log(err.message);
      }
    } else {
    }
  }

  return (
    <>
      {localStorage.getItem("user") ? (
        <>
          {/* <div>
            {" "}
            {localStorage.getItem("user")}
            <button onClick={handleFetch}> click here</button>{" "}
          </div> */}
          <div style={{ padding: "10px", marginTop: "10px" }}>
            <Box bgGradient={["linear(to-tr, white.100, black.200)"]}>
              <Table variant="striped" colorScheme="teal">
                {/* <TableCaption>
                  Imperial to metrsdsic conversion factors
                </TableCaption> */}
                <Thead>
                  <Tr>
                    <Th width="25%">Song Name</Th>
                    <Th>We think you will like it because...</Th>
                    <Th>Link</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {recommendations.map((row, index) => (
                    <Tr key={index}>
                      <Td
                        style={{
                          borderRight: "1px solid black",
                          borderBottom: "1px solid black",
                        }}
                      >
                        {" "}
                        {row}
                      </Td>
                      <Td
                        style={{
                          borderRight: "1px solid black",
                          borderBottom: "1px solid black",
                        }}
                      >
                        {" "}
                        {recExplanations[index]}
                      </Td>
                      <Td
                        style={{
                          borderRight: "1px solid black",
                          borderBottom: "1px solid black",
                        }}
                      >
                        {" "}
                        <a
                          href={songLinks[index]}
                          target="_blank"
                          rel="noopener noreferrer"
                          style={{ color: "blue", textDecoration: "underline" }}
                        >
                          {songLinks[index]}
                        </a>
                      </Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            </Box>
          </div>
        </>
      ) : (
        <div> you are not logged in yet!</div>
      )}
    </>
  );
}

export default OurPicks;
