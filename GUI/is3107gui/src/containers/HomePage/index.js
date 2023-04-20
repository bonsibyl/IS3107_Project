import { useState, useEffect } from "react";
import {
  Container,
  SimpleGrid,
  Image,
  Flex,
  Heading,
  Text,
  Stack,
  StackDivider,
  Icon,
  useColorModeValue,
  Button,
  Input,
  Box,
} from "@chakra-ui/react";
import {
  IoAnalyticsSharp,
  IoLogoBitcoin,
  IoSearchSharp,
} from "react-icons/io5";
import { ReactElement } from "react";
import FetchData from "../../components/FetchData";
import { useNavigate } from "react-router-dom";
import { supabase } from "../../client";

interface FeatureProps {
  text: string;
  iconBg: string;
  icon?: ReactElement;
}
const Feature = ({ text, icon, iconBg }: FeatureProps) => {
  return (
    <Stack direction={"row"} align={"center"}>
      <Flex
        w={8}
        h={8}
        align={"center"}
        justify={"center"}
        rounded={"full"}
        bg={iconBg}
      >
        {icon}
      </Flex>
      <Text fontWeight={600}>{text}</Text>
    </Stack>
  );
};

export default function HomePage(props) {
  const [users, setUsers] = useState([]);
  const [spotifyUrl, setSpotifyUrl] = useState("");
  const navigate = useNavigate();

  // useEffect(() => {
  //   if (!user) {
  //     navigate("/login");
  //   }
  // }, [user]);

  // useEffect(() => {
  //   handleFetch();
  // }, []);

  async function handleFetch() {
    //e.preventDefault();
    try {
      const response = await fetch("http://localhost:5001/user_data");
      // console.log(response.status);
      // console.log(response.text());
      const jsonData = await response.json();
      console.log("jsonData: ", jsonData);

      setUsers(jsonData);
      console.log(users[0]);
      console.log(users.username);
    } catch (err) {
      console.log(err.message);
    }
  }

  async function handleSubmit() {
    const username = localStorage.getItem("user");
    console.log(username, spotifyUrl);
    const response = await fetch("http://localhost:5001/spotifyUrl", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ username, spotifyUrl }),
    });

    if (response.ok) {
      console.log("Url updated success");
      //localStorage.setItem("user", JSON.stringify(username));
      localStorage.setItem("user", username);
      //alert("Login success! Welcome " + username);
      navigate("/ourPicks");
    } else {
      console.log("invalid url");
      alert("Invalid url");
      navigate("/");
    }
  }

  return (
    <div>
      <Container maxW={"5xl"} py={12}>
        <SimpleGrid columns={{ base: 1, md: 2 }} spacing={10}>
          <Stack spacing={4}>
            <Heading>Welcome to Spotified!</Heading>
            <Text color="gray.500" mb="8px">
              Spotify Playlist Url:{" "}
            </Text>
            <Flex>
              <Input
                value={spotifyUrl}
                onChange={(event) => setSpotifyUrl(event.target.value)}
                placeholder="Enter playlist url here"
                size="md"
              />
              <Button colorScheme="teal" onClick={handleSubmit}>
                {" "}
                Enter
              </Button>
            </Flex>
            <Text color={"gray.900"} fontSize={"lg"}>
              Get personalised insights, know what songs you love and why you
              love them! <br /> Spotified is the music buddy that you never knew
              you needed... till now.
            </Text>
            <Stack
              spacing={4}
              divider={
                <StackDivider
                  borderColor={useColorModeValue("gray.100", "gray.700")}
                />
              }
            ></Stack>
          </Stack>
          <Flex>
            <Image
              rounded={"md"}
              alt={"feature image"}
              src={
                "https://images.unsplash.com/photo-1593698054589-8c14bb66d2d4?ixlib=rb-4.0.3&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=1470&q=80        "
              }
              objectFit={"cover"}
            />
          </Flex>
        </SimpleGrid>
      </Container>

      {/* <div>
        <h1>Welcome to the homepage!</h1>
        <Button onClick={handleFetch}> click here</Button>
        {users && (
          <div>
            {" "}
            <p> Username: {users.username} </p>{" "}
          </div>
        )}
        <div> {hello}</div>
        <FetchData visData={props.visData} />
      </div>

      <div>
        {" "}
        <Button onClick={handleStore}> clickhere </Button>
      </div> */}
    </div>
  );
}