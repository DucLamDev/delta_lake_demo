import React, { useEffect, useState } from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  Container,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  CircularProgress,
} from "@mui/material";

const apiEndpoints = [
  { label: "Simple Example", url: "/api/delta/simple" },
  { label: "Update Example", url: "/api/delta/update" },
  { label: "Versioning Example", url: "/api/delta/versioning" },
  { label: "Delete Example", url: "/api/delta/delete" },
  { label: "Complex Example", url: "/api/delta/complex" },
];

const DeltaData = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedApi, setSelectedApi] = useState(apiEndpoints[0].url);

  const fetchData = (apiUrl) => {
    setLoading(true);
    setError(null);

    fetch(`http://localhost:5000${apiUrl}`)
      .then((response) => {
        if (!response.ok) throw new Error("Failed to fetch data");
        return response.json();
      })
      .then((data) => setData(data))
      .catch((error) => setError(error.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchData(selectedApi);
  }, [selectedApi]);

  return (
    <div>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" style={{ flexGrow: 1 }}>
            Delta Lake API Viewer
          </Typography>
        </Toolbar>
      </AppBar>

      <Container style={{ marginTop: "20px" }}>
        <Typography variant="h5" gutterBottom>
          Choose an API to Fetch Data
        </Typography>

        <div style={{ display: "flex", gap: "10px", marginBottom: "20px" }}>
          {apiEndpoints.map((api) => (
            <Button
              key={api.url}
              variant={selectedApi === api.url ? "contained" : "outlined"}
              onClick={() => setSelectedApi(api.url)}
              style={{ textTransform: "none" }}
            >
              {api.label}
            </Button>
          ))}
        </div>

        {loading && <CircularProgress />}
        {error && <Typography color="error">Error: {error}</Typography>}

        {data && (
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  {Object.keys(data[0] || {}).map((key) => (
                    <TableCell key={key}>{key}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {data.map((row, index) => (
                  <TableRow key={index}>
                    {Object.values(row).map((value, i) => (
                      <TableCell key={i}>{value}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Container>
    </div>
  );
};

export default DeltaData;
