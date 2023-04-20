import React, { useContext } from "react";
import { DataContext } from "../../App";
import {
  Grid,
  GridItem,
  IconButton,
  Flex,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalCloseButton,
  useDisclosure,
  Box,
} from "@chakra-ui/react";
import { ExternalLinkIcon } from "@chakra-ui/icons";
import { TagCloud } from "react-tagcloud";
import { Radar, Bar } from "react-chartjs-2";
import { SocialIcon } from "react-social-icons";
import { FaMusic } from "react-icons/fa";
import {
  Chart as ChartJS,
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  BarElement,
} from "chart.js";
import { redirect } from "react-router-dom";
ChartJS.register(
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  BarElement
);

function Dashboard(props) {
  const { visData } = useContext(DataContext);

  //radar chart
  const data = {
    labels: [
      "danceability",
      "energy",
      "acousticness",
      "instrumentalness",
      "liveness",
      "valence",
    ],
    datasets: [
      {
        label: "mean values",
        data: [
          visData.danceability,
          visData.energy,
          visData.acousticness,
          visData.instrumentalness,
          visData.liveness,
          visData.valence,
        ],
        backgroundColor: "rgba(255, 99, 132, 0.2)",
        borderColor: "rgba(255, 99, 132, 1)",
        borderWidth: 2,
        pointHoverBackgroundColor: "#fff",
        pointHoverBorderColor: "rgb(255, 99, 132)",
      },
    ],
  };

  const options = {
    scales: {
      r: {
        pointLabels: {
          fontSize: 30,
        },
      },
    },
  };

  //word cloud
  const dict =
    visData && visData.genres
      ? Object.entries(visData.genres).map(([genre, count], index) => ({
          value: genre,
          count: count,
        }))
      : [];

  const buckets = {
    pop: 0,
    "r&b": 0,
    jazz: 0,
    classical: 0,
    indie: 0,
    rock: 0,
    country: 0,
    funk: 0,
    disco: 0,
    "k-pop": 0,
    metal: 0,
    rap: 0,
  };

  dict.forEach(({ value, count }) => {
    if (value.includes("pop")) {
      buckets["pop"] += count;
    }
    if (value.includes("r&b")) {
      buckets["r&b"] += count;
    }
    if (value.includes("jazz")) {
      buckets["jazz"] += count;
    }
    if (value.includes("classical")) {
      buckets["classical"] += count;
    }
    if (value.includes("indie")) {
      buckets["indie"] += count;
    }
    if (value.includes("rock")) {
      buckets["rock"] += count;
    }
    if (value.includes("country")) {
      buckets["country"] += count;
    }
    if (value.includes("funk")) {
      buckets["funk"] += count;
    }
    if (value.includes("disco")) {
      buckets["disco"] += count;
    }
    if (value.includes("k-pop")) {
      buckets["k-pop"] += count;
    }
    if (value.includes("metal")) {
      buckets["metal"] += count;
    }
    if (value.includes("rap")) {
      buckets["rap"] += count;
    }
  });

  const cloudData = Object.entries(buckets).map(([value, count], index) => ({
    value: value,
    count: count,
  }));

  const customRenderer = (tag, size, color) => (
    <span
      key={tag.value}
      style={{
        animation: "blinker 3s linear infinite",
        animationDelay: `${Math.random() * 2}s`,
        // fontSize: fontSize(size),
        fontSize: `${(size / 5) * 3}em`,
        border: `2px solid ${color}`,
        margin: "3px",
        padding: "3px",
        display: "inline-block",
        color: "white",
        animationName: {
          "@keyframes blinker": {
            "0%": { opacity: 1 },
            "50%": { opacity: 0.5 },
            "100%": { opacity: 1 },
          },
        },
      }}
    >
      {tag.value}
    </span>
  );

  //pictorial data for key
  const PictorialChart = ({ value, max, iconSize, iconSpacing }) => {
    const stars = [];
    const containerWidth = (iconSize + iconSpacing) * max - iconSpacing;

    for (let i = 0; i < max; i++) {
      const color = i < value - 1 ? "papayawhip" : "gray";
      stars.push(
        <FaMusic
          key={i}
          color={color}
          size={`${iconSize}px`}
          style={{ display: "inline-block" }}
        />
      );
    }

    return <div style={{ display: "flex", alignItems: "center" }}>{stars}</div>;
  };

  //bar chart for loudness and tempo
  const barOptions = {
    indexAxis: "y",
    elements: {
      bar: {
        borderWidth: 3,
      },
    },
    responsive: true,
    plugins: {
      legend: {
        position: "right",
      },
    },
  };

  const labels = [" "];

  const barData = {
    labels,
    datasets: [
      {
        label: "Loudness",
        data: labels.map(() => visData.loudness),
        borderColor: "rgb(255, 99, 132)",
        backgroundColor: "rgba(255, 99, 132, 0.5)",
      },
      {
        label: "Tempo",
        data: labels.map(() => visData.tempo),
        borderColor: "rgb(53, 162, 235)",
        backgroundColor: "rgba(53, 162, 235, 0.5)",
      },
    ],
  };

  //modal to share on social media
  const { isOpen, onOpen, onClose } = useDisclosure();

  return (
    <Box
      style={{ marginTop: "-30px" }}
      bgGradient={["linear(to-tr, teal.100, pink.200)"]}
    >
      <Flex justifyContent="flex-end" mr={7} mt={7}>
        <IconButton
          colorScheme="transparent"
          aria-label="Search database"
          size="lg"
          icon={<ExternalLinkIcon boxSize={10} />}
          onClick={onOpen}
        />
      </Flex>
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Share my Dashboard!</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Flex flexWrap="wrap">
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="instagram"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="facebook"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="reddit"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="tiktok"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="twitter"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
              <Box mb={1} mr={2}>
                <SocialIcon
                  network="snapchat"
                  style={{ height: 70, width: 70 }}
                />
              </Box>
            </Flex>
          </ModalBody>
        </ModalContent>
      </Modal>
      <Grid templateColumns="repeat(4, 1fr)" gap={6}>
        <GridItem
          h="400"
          colStart={2}
          colEnd={4}
          bg="salmon"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexDirection: "column",
            border: "10px solid white",
          }}
        >
          <h1 style={{ fontSize: "30px", fontFamily: "Berlin Sans FB" }}>
            Here are your genres in your playlist!
          </h1>
          <TagCloud
            tags={cloudData}
            renderer={customRenderer}
            minSize={3}
            maxSize={7}
          />
          <style>
            {`@keyframes blinker {
        0% {
          opacity: 0;
        }
        50% {
          opacity: 0.5;
        }
        100% {
          opacity: 1;
        }
      }`}
          </style>
        </GridItem>

        <GridItem
          h="500"
          colStart={2}
          colEnd={4}
          bg="papayawhip"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexDirection: "column",
            border: "10px solid white",
          }}
        >
          <Radar data={data} options={options} />
        </GridItem>
        <GridItem
          h="250px"
          colStart={2}
          colEnd={4}
          bg="salmon"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexDirection: "column",
            border: "10px solid white",
          }}
        >
          <h1 style={{ fontSize: "30px", fontFamily: "Berlin Sans FB" }}>
            Here is your average song key value!
          </h1>
          <PictorialChart
            value={visData.key}
            max={11}
            iconSize={50}
            iconSpacing={10}
          />
        </GridItem>
        <GridItem
          h="550"
          colStart={2}
          colEnd={4}
          bg="papayawhip"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexDirection: "column",
            border: "10px solid white",
          }}
        >
          <h1 style={{ fontSize: "30px", fontFamily: "Berlin Sans FB" }}>
            This is how you have been grooving...
          </h1>
          <Bar options={barOptions} data={barData} />
        </GridItem>
      </Grid>
    </Box>
  );
}

export default Dashboard;
